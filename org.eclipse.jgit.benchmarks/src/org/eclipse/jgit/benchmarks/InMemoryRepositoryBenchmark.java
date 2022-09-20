package org.eclipse.jgit.benchmarks;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryDescription;
import org.eclipse.jgit.internal.storage.dfs.InMemoryRepository;
import org.eclipse.jgit.lib.CommitBuilder;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectInserter;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.RefUpdate;
import org.eclipse.jgit.revwalk.RevCommit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
public class InMemoryRepositoryBenchmark {
    InMemoryRepository repo;

    ObjectId commit;



    @Param({ "100", "1000", "4000"})
    int iterations;

    @Setup
    public void setupBenchmark() throws Exception {
        repo = new InMemoryRepository(
                new DfsRepositoryDescription());

        ObjectInserter oi = repo.newObjectInserter();
        CommitBuilder cb  = new CommitBuilder();
        PersonIdent person = new PersonIdent("joe", "joe@invalid");
        cb.setAuthor(person);
        cb.setCommitter(person);
        cb.setTreeId(Constants.EMPTY_TREE_ID);
        cb.setMessage("EMPTY");
        commit = oi.insert(cb);
    }

    /* quick & dirty benchmark. With 4000 iterations, online log
   compaction is 4x faster than previous strategy. */
    @Benchmark
    @BenchmarkMode({Mode.SingleShotTime})
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Fork(value = 1, warmups = 1)
    @Warmup(iterations = 1)
    @Measurement(iterations = 1)
    public void manyBatchRefUpdates() throws Exception {
        String refname = "";
        for (int i = 0; i < iterations; i++) {
            Ref last = repo.getRefDatabase().exactRef(refname);
            ObjectId c = last == null ? commit : last.getObjectId();

            refname = "refs/branch" + i + String.join("", Collections.nCopies(2000, "x"));
            RefUpdate ru = repo.getRefDatabase().newUpdate(refname, false);
            ru.setNewObjectId(c);
            ru.setForceUpdate(true);
            RefUpdate.Result res = RefUpdate.Result.FORCED;
            if (res != RefUpdate.Result.FORCED)
                throw new IllegalStateException("failed refupdate:" + res);
        }
    }
}
