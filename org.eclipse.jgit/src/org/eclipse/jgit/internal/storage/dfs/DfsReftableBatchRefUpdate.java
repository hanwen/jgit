/*
 * Copyright (C) 2019, Google Inc. and others
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Distribution License v. 1.0 which is available at
 * https://www.eclipse.org/org/documents/edl-v10.php.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */

package org.eclipse.jgit.internal.storage.dfs;

import org.eclipse.jgit.internal.storage.dfs.DfsObjDatabase.PackSource;
import org.eclipse.jgit.internal.storage.io.BlockSource;
import org.eclipse.jgit.internal.storage.pack.PackExt;
import org.eclipse.jgit.internal.storage.reftable.ReftableBatchRefUpdate;
import org.eclipse.jgit.internal.storage.reftable.ReftableCompactor;
import org.eclipse.jgit.internal.storage.reftable.ReftableConfig;
import org.eclipse.jgit.internal.storage.reftable.ReftableReader;
import org.eclipse.jgit.internal.storage.reftable.ReftableWriter;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.transport.ReceiveCommand;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.eclipse.jgit.internal.storage.pack.PackExt.REFTABLE;

/**
 * {@link org.eclipse.jgit.lib.BatchRefUpdate} for
 * {@link org.eclipse.jgit.internal.storage.dfs.DfsReftableDatabase}.
 */
public class DfsReftableBatchRefUpdate extends ReftableBatchRefUpdate {
	private static final int AVG_BYTES = 36;

	private final DfsReftableDatabase refdb;

	private final DfsObjDatabase odb;

	/**
	 * Initialize batch update.
	 *
	 * @param refdb
	 *            database the update will modify.
	 * @param odb
	 *            object database to store the reftable.
	 */
	protected DfsReftableBatchRefUpdate(DfsReftableDatabase refdb,
			DfsObjDatabase odb) {
		super(refdb, refdb.reftableDatabase, refdb.getLock(), refdb.getRepository());
		this.refdb = refdb;
		this.odb = odb;
	}

	@Override
	protected void applyUpdates(List<Ref> newRefs, List<ReceiveCommand> pending)
			throws IOException {
		Set<DfsPackDescription> prune = new HashSet<>();
		DfsPackDescription pack = odb.newPack(PackSource.INSERT);
		try (DfsOutputStream out = odb.writeFile(pack, REFTABLE)) {
			ReftableConfig cfg = DfsPackCompactor
					.configureReftable(refdb.getReftableConfig(), out);

			ReftableWriter.Stats stats;
			ByteArrayOutputStream tmp = new ByteArrayOutputStream();
			ReftableWriter rw = new ReftableWriter(cfg, tmp);
			write(rw, newRefs, pending);
			rw.finish();
			stats = compactTopOfStack(out, cfg, tmp.toByteArray(), rw.getStats(), prune);
			pack.addFileExt(REFTABLE);
			pack.setReftableStats(stats);
		}

		odb.commitPack(Collections.singleton(pack), prune);
		odb.addReftable(pack, prune);
		refdb.clearCache();
	}

	private boolean canCompactTopOfStack(ReftableConfig cfg)
			throws IOException {
		refdb.getLock().lock();
		try {
			DfsReftableStack stack = refdb.stack();
			List<ReftableReader> readers = stack.readers();
			if (readers.isEmpty()) {
				return false;
			}

			int lastIdx = readers.size() - 1;
			DfsReftable last = stack.files().get(lastIdx);
			DfsPackDescription desc = last.getPackDescription();
			if (desc.getPackSource() != PackSource.INSERT
				|| !packOnlyContainsReftable(desc)) {
				return false;
			}

			ReftableReader table = readers.get(lastIdx);
			int bs = cfg.getRefBlockSize();
			return table.size() <= 3 * bs;
		} finally {
			refdb.getLock().unlock();
		}
	}

	final static int OVERHEAD = 91;
	private int log2(long sz) {
		int l = 0;
		while (sz > 0) {
			l++;
			sz /= 2;
		}
		return l;
	}

	private ReftableWriter.Stats compactTopOfStack(OutputStream out,
			ReftableConfig cfg, byte[] newTable, ReftableWriter.Stats newStats, Set<DfsPackDescription> toPrune) throws IOException {

		refdb.getLock().lock();
		try {
			DfsReftableStack dfsReftableStack = refdb.stack();
			List<ReftableReader> stack = dfsReftableStack.readers();

			/* Try to find a tail section that (if compacted) yields a larger log2 size.
			 * This means that for each compaction step, each ref is in a differently sized table (in terms of log2)
			 * limiting total work to O(#refs * log2(#refs)). (It's probably linear, but too lazy to prove it.)
			 */
			long cumulative_size = 0;
			long max_log2 = 0;
			int compact_segment_start = stack.size();
			for (int i = stack.size(); i >= 0; i--) {
				if (!refdb.compactDuringCommit())
					break;

				long sz;
				if (i == stack.size()) {
					sz = newTable.length;
				} else if (!packOnlyContainsReftable(dfsReftableStack.files().get(i).desc)) {
					// NOSUBMIT - what about INSERT packtype?
					break;
				} else {
					sz = stack.get(i).size();
				}
				sz -= OVERHEAD;
				cumulative_size += sz;
				if (log2(sz) > max_log2) {
					max_log2 = log2(sz);
				}
				// NOSUBMIT - should we also compact if cumulative_size < blocksize ?
				if (log2(cumulative_size) > max_log2) {
					compact_segment_start = i;
				}
			}

			if (compact_segment_start == stack.size()) {
				out.write(newTable);
				return newStats;
			}
			List<ReftableReader> tables = new ArrayList<>();
			for (int i = compact_segment_start; i < stack.size(); i++) {
				tables.add(stack.get(i));
				toPrune.add(dfsReftableStack.files().get(i).desc);
			}
			tables.add(new ReftableReader(BlockSource.from(newTable)));

			ReftableCompactor compactor = new ReftableCompactor(out);
			compactor.setConfig(cfg);
			compactor.setIncludeDeletes(true);
			compactor.addAll(tables);
			compactor.compact();
			return compactor.getStats();
		} finally {
			refdb.getLock().unlock();
		}
	}

	private boolean packOnlyContainsReftable(DfsPackDescription desc) {
		for (PackExt ext : PackExt.values()) {
			if (ext != REFTABLE && desc.hasFileExt(ext)) {
				return false;
			}
		}
		return true;
	}
}
