#include "PathWatcher.h"

#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

class TestPathWatcher : public CppUnit::TestCase {
public:
    CPPUNIT_TEST_SUITE(TestPathWatcher);
    CPPUNIT_TEST(testFileModified);
    CPPUNIT_TEST(testFileCreated);
    CPPUNIT_TEST(testFileDeleted);
    CPPUNIT_TEST(testFileMoved);
    CPPUNIT_TEST_SUITE_END();

public:
    std::string tempDir;

    TestPathWatcher() {
        std::ostringstream dir;
        time_t ts = time(NULL);
        dir << "/tmp/scribe-test-" << ts;
        tempDir = dir.str();
        mkdir(tempDir.c_str(), S_IRWXU | S_IRWXG);
    }

    void testFileModified() {
        // Create and watch a file.
        std::string file = tempDir + "/test.txt";
        std::fstream f(file.c_str(), std::ios_base::out);
        f << "line1" << std::endl;
        PathWatcher pathWatcher;
        std::cout << "Attempting to watch " << file << std::endl;
        CPPUNIT_ASSERT(pathWatcher.tryWatchFile(file));

        // Modify the file.
        f << "line2" << std::endl;

        // Make sure we're notified.
        bool fileEvent, rewatch;
        pathWatcher.waitForEvent(fileEvent, rewatch);
        CPPUNIT_ASSERT(fileEvent);
        CPPUNIT_ASSERT(!rewatch);
        f.close();
        unlink(file.c_str());
    }

    void testFileCreated() {
        std::string file = tempDir + "/test.txt";
        PathWatcher pathWatcher;

        // Watch a non-existing file.
        std::cout << "Attempting to watch " << file << std::endl;
        CPPUNIT_ASSERT(!pathWatcher.tryWatchFile(file));
        CPPUNIT_ASSERT(pathWatcher.tryWatchDirectory(tempDir));

        // Create the file.
        std::fstream f(file.c_str(), std::ios_base::out);
        f << "line1" << std::endl;
        bool fileEvent, rewatch;

        // Make sure we're notified of the file creation.
        pathWatcher.waitForEvent(fileEvent, rewatch);
        CPPUNIT_ASSERT(!fileEvent);
        CPPUNIT_ASSERT(rewatch);

        // Make sure we're notified when the file changes.
        CPPUNIT_ASSERT(pathWatcher.tryWatchFile(file));
        f << "line2" << std::endl;
        pathWatcher.waitForEvent(fileEvent, rewatch);
        CPPUNIT_ASSERT(fileEvent);
        f.close();
        unlink(file.c_str());
    }

    void testFileDeleted() {
        std::string file = tempDir + "/test.txt";
        
        // Create and watch a file.
        std::fstream f(file.c_str(), std::ios_base::out);
        f << "line1" << std::endl;
        PathWatcher pathWatcher;
        std::cout << "Attempting to watch " << file << std::endl;
        CPPUNIT_ASSERT(pathWatcher.tryWatchFile(file));

        // Delete the file.
        unlink(file.c_str());

        // Make sure we're notified.
        bool fileEvent, rewatch;
        pathWatcher.waitForEvent(fileEvent, rewatch);
        CPPUNIT_ASSERT(!fileEvent);
        CPPUNIT_ASSERT(rewatch);
        f.close();
    }

    void testFileMoved() {
        // Create and watch a file 'test.txt'.
        std::string file = tempDir + "/test.txt";
        std::fstream f(file.c_str(), std::ios_base::out);
        f << "line1" << std::endl;
        f.close();

        PathWatcher pathWatcher;
        std::cout << "Attempting to watch " << file << std::endl;
        CPPUNIT_ASSERT(pathWatcher.tryWatchFile(file));

        // Move 'test.txt' to 'moved.txt'.
        std::string movedFile = tempDir + "/moved.txt";
        rename(file.c_str(), movedFile.c_str());
        bool fileEvent, rewatch;
        pathWatcher.waitForEvent(fileEvent, rewatch);
        CPPUNIT_ASSERT(fileEvent);
        CPPUNIT_ASSERT(rewatch);
        CPPUNIT_ASSERT(!pathWatcher.tryWatchFile(file));
        CPPUNIT_ASSERT(pathWatcher.tryWatchDirectory(tempDir));

        // Move 'moved.txt' back to 'test.txt'.
        rename(movedFile.c_str(), file.c_str());
        pathWatcher.waitForEvent(fileEvent, rewatch);
        CPPUNIT_ASSERT(!fileEvent);
        CPPUNIT_ASSERT(rewatch);
        CPPUNIT_ASSERT(pathWatcher.tryWatchFile(file));

        // Make sure we still get notified on change to 'test.txt'.
        std::fstream f2(file.c_str(), std::ios_base::out);
        f2 << "line2" << std::endl;
        pathWatcher.waitForEvent(fileEvent, rewatch);
        CPPUNIT_ASSERT(fileEvent);
        CPPUNIT_ASSERT(!rewatch);
        f2.close();
        unlink(file.c_str());
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(TestPathWatcher);

int main(int argc, char **argv)
{
  CppUnit::TextUi::TestRunner runner;
  CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
  runner.addTest( registry.makeTest() );
  runner.run();
  return 0;
}
