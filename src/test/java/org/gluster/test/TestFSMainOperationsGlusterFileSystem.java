package org.gluster.test;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.fs.FSMainOperationsBaseTest;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Path;
import org.apache.tools.ant.util.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TestFSMainOperationsGlusterFileSystem extends
        FSMainOperationsBaseTest {

    @Override
    @Ignore
    @Test
    public void testRenameDirectoryAsNonExistentDirectory() throws Exception {
        // TODO Auto-generated method stub
        super.testRenameDirectoryAsNonExistentDirectory();
    }

    @Override
    @Ignore
    @Test
    public void testListStatus() throws Exception {
        // TODO Auto-generated method stub
        super.testListStatus();
    }

    @Override
    @Ignore
    @Test
    public void testWorkingDirectory() throws Exception {
        // TODO Auto-generated method stub
        super.testWorkingDirectory();
    }

    @Override
    @Ignore
    @Test
    public void testGlobStatusFilterWithEmptyPathResults() throws Exception {
        // TODO Auto-generated method stub
        super.testGlobStatusFilterWithEmptyPathResults();
    }

    @Override
    @Ignore
    @Test
    public void testGlobStatusFilterWithMultiplePathMatchesAndNonTrivialFilter()
            throws Exception {
        // TODO Auto-generated method stub
        super.testGlobStatusFilterWithMultiplePathMatchesAndNonTrivialFilter();
    }

    @Override
    @Ignore
    @Test
    public void testGlobStatusFilterWithMultiplePathWildcardsAndNonTrivialFilter()
            throws Exception {
        // TODO Auto-generated method stub
        super.testGlobStatusFilterWithMultiplePathWildcardsAndNonTrivialFilter();
    }

    @Override
    @Ignore
    @Test
    public void testGlobStatusFilterWithMultipleWildCardMatchesAndTrivialFilter()
            throws Exception {
        // TODO Auto-generated method stub
        super.testGlobStatusFilterWithMultipleWildCardMatchesAndTrivialFilter();
    }

    @Override
    @Ignore
    @Test
    public void testGlobStatusFilterWithNoMatchingPathsAndNonTrivialFilter()
            throws Exception {
        // TODO Auto-generated method stub
        super.testGlobStatusFilterWithNoMatchingPathsAndNonTrivialFilter();
    }

    @Override
    @Ignore
    @Test
    public void testGlobStatusFilterWithSomePathMatchesAndTrivialFilter()
            throws Exception {
        // TODO Auto-generated method stub
        super.testGlobStatusFilterWithSomePathMatchesAndTrivialFilter();
    }

    @Override
    @Ignore
    @Test
    public void testGlobStatusSomeMatchesInDirectories() throws Exception {
        // TODO Auto-generated method stub
        super.testGlobStatusSomeMatchesInDirectories();
    }

    @Override
    @Ignore
    @Test
    public void testGlobStatusThrowsExceptionForNonExistentFile()
            throws Exception {
        // TODO Auto-generated method stub
        super.testGlobStatusThrowsExceptionForNonExistentFile();
    }

    @Override
    @Ignore
    @Test
    public void testGlobStatusWithMultipleMatchesOfSingleChar()
            throws Exception {
        // TODO Auto-generated method stub
        super.testGlobStatusWithMultipleMatchesOfSingleChar();
    }

    @Override
    @Ignore
    @Test
    public void testGlobStatusWithMultipleWildCardMatches() throws Exception {
        // TODO Auto-generated method stub
        super.testGlobStatusWithMultipleWildCardMatches();
    }

    @Override
    @Ignore
    @Test
    public void testGlobStatusWithNoMatchesInPath() throws Exception {
        // TODO Auto-generated method stub
        super.testGlobStatusWithNoMatchesInPath();
    }

    @Override
    @Ignore
    @Test
    public void testMkdirs() throws Exception {
        // TODO Auto-generated method stub
        super.testMkdirs();
    }

    public void clean() throws IOException {
        /**
         * By clearing these before starting tests, we gaurantee a clean
         * environment. This is required for about 50% of the tests in the
         * superclass.
         */
        fSys.delete(new Path("/mapred/"));
        fSys.delete(new Path("/build/"));
        fSys.delete(new Path("/test/"));
        fSys.mkdirs(new Path("/test/hadoop"));
    }

    @Before
    public void setUp() throws Exception {
        fSys = GFSUtil.create(true);
        clean();
        super.setUp();
    }

    static Path wd = null;

    protected Path getDefaultWorkingDirectory() throws IOException {
        if (wd == null)
            wd = fSys.getWorkingDirectory();
        return wd;
    }

    @After
    public void tearDown() throws Exception {
        fSys.close();
        FileUtils.delete(GFSUtil.getTempDirectory());
    }

    @Test
    @Ignore
    @Override
    public void testWDAbsolute() throws IOException {
        Path absoluteDir = FileSystemTestHelper.getTestRootPath(fSys,
                "test/existingDir");
        fSys.mkdirs(absoluteDir);
        fSys.setWorkingDirectory(absoluteDir);
        Assert.assertEquals(absoluteDir, fSys.getWorkingDirectory());
    }
}
