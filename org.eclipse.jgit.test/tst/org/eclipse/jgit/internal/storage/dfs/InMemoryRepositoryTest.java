/*
 * Copyright (C) 2019, Google LLC. and others
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Distribution License v. 1.0 which is available at
 * https://www.eclipse.org/org/documents/edl-v10.php.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */
package org.eclipse.jgit.internal.storage.dfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import org.eclipse.jgit.internal.storage.pack.PackExt;
import org.eclipse.jgit.junit.TestRepository;
import org.eclipse.jgit.lib.ObjectIdRef;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Ref.Storage;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTag;
import org.junit.Test;

public class InMemoryRepositoryTest {

	@Test
	public void keepUpdateIndexPeelingTag() throws Exception {
		InMemoryRepository repo = new InMemoryRepository(
				new DfsRepositoryDescription());
		try (TestRepository<InMemoryRepository> git = new TestRepository<>(
				repo)) {
			RevCommit commit = git.branch("master").commit()
					.message("first commit").create();
			RevTag tag = git.tag("v0.1", commit);
			git.update("refs/tags/v0.1", tag);

			Ref unpeeledTag = new ObjectIdRef.Unpeeled(Storage.LOOSE,
					"refs/tags/v0.1", tag.getId(), 1000);

			Ref peeledTag = repo.getRefDatabase().peel(unpeeledTag);
			assertTrue(peeledTag instanceof ObjectIdRef.PeeledTag);
			assertEquals(1000, peeledTag.getUpdateIndex());
		}
	}

	@Test
	public void keepUpdateIndexPeelingNonTag() throws Exception {
		InMemoryRepository repo = new InMemoryRepository(
				new DfsRepositoryDescription());
		try (TestRepository<InMemoryRepository> git = new TestRepository<>(
				repo)) {
			RevCommit commit = git.branch("master").commit()
					.message("first commit").create();

			Ref unpeeledRef = new ObjectIdRef.Unpeeled(Storage.LOOSE,
					"refs/heads/master", commit.getId(), 1000);
			Ref peeledRef = repo.getRefDatabase().peel(unpeeledRef);
			assertTrue(peeledRef instanceof ObjectIdRef.PeeledNonTag);
			assertEquals(1000, peeledRef.getUpdateIndex());
		}
	}

	@Test
	public void sha1ToTip_ref() throws Exception {
		InMemoryRepository repo = new InMemoryRepository(
				new DfsRepositoryDescription());
		try (TestRepository<InMemoryRepository> git = new TestRepository<>(
				repo)) {
			RevCommit commit = git.branch("master").commit()
					.message("first commit").create();

			Set<Ref> tipsWithSha1 = repo.getRefDatabase()
					.getTipsWithSha1(commit.getId());
			assertEquals(1, tipsWithSha1.size());
			Ref ref = tipsWithSha1.iterator().next();
			assertEquals(ref.getName(), "refs/heads/master");
			assertEquals(commit.getId(), ref.getObjectId());
		}
	}

	@Test
	public void sha1ToTip_annotatedTag() throws Exception {
		InMemoryRepository repo = new InMemoryRepository(
				new DfsRepositoryDescription());
		try (TestRepository<InMemoryRepository> git = new TestRepository<>(
				repo)) {
			RevCommit commit = git.commit()
					.message("first commit").create();
			RevTag tagObj = git.tag("v0.1", commit);
			git.update("refs/tags/v0.1", tagObj);
			Set<Ref> tipsWithSha1 = repo.getRefDatabase()
					.getTipsWithSha1(commit.getId());
			assertEquals(1, tipsWithSha1.size());
			Ref ref = tipsWithSha1.iterator().next();
			assertEquals(ref.getName(), "refs/tags/v0.1");
			assertEquals(commit.getId(), ref.getPeeledObjectId());
		}
	}

	@Test
	public void sha1ToTip_tag() throws Exception {
		InMemoryRepository repo = new InMemoryRepository(
				new DfsRepositoryDescription());
		try (TestRepository<InMemoryRepository> git = new TestRepository<>(
				repo)) {
			RevCommit commit = git.commit().message("first commit").create();
			git.update("refs/tags/v0.2", commit);
			Set<Ref> tipsWithSha1 = repo.getRefDatabase()
					.getTipsWithSha1(commit.getId());
			assertEquals(1, tipsWithSha1.size());
			Ref ref = tipsWithSha1.iterator().next();
			assertEquals(ref.getName(), "refs/tags/v0.2");
			assertEquals(commit.getId(), ref.getObjectId());
		}
	}

	@Test
	public void onlineCompaction() throws Exception {
		InMemoryRepository repo = new InMemoryRepository(
				new DfsRepositoryDescription());

		/* quick & dirty benchmark. With 4000 iterations, online log
		   compaction is 4x faster than previous strategy. */
		boolean benchmark = false;
		long start = System.nanoTime();
		try (TestRepository<InMemoryRepository> git = new TestRepository<>(
				repo)) {
			for (int i = 0; i < 128; i++) {
				RevCommit commit = git.commit().message("commit no " + i).create();
				/* Must have at least 2 refs per
				   block, or the reftable code barfs
				   trying to write the index. */
				git.update("refs/branch" + i + String.join("", Collections.nCopies(2000, "x")), commit);
				if (!benchmark) {
					int count = 0;
					for (DfsPackDescription desc : repo.getObjectDatabase().listPacks()) {
						if (desc.hasFileExt(PackExt.REFTABLE)) count++;
					}
					// 128 refs of size 1/2 block = 64 blocks = 2^6.
					// Maximum config is 5 reftables, corresponding to
					// one table of 2^5 blocks, one of 2^4 etc. */
					assertTrue(count < 6);
				}
			}
		}
		long end = System.nanoTime();
		if (benchmark)
		  System.err.println("dt = " + (end - start));
	}
}
