//  Copyright (c) 2007-2008 Facebook
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
// See accompanying file LICENSE or visit the Scribe site at:
// http://developers.facebook.com/scribe/
//
// @author Bobby Johnson
// @author James Wang
// @author Jason Sobel
// @author Alex Moskalyuk
// @author Avinash Lakshman
// @author Anthony Giardullo
// @author Jan Oravec

#include "common.h"
#include "scribe_server.h"
#include "thrift/transport/TSimpleFileTransport.h"

using namespace std;
using namespace boost;
using namespace boost::filesystem;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;
using namespace scribe::thrift;

#define DEFAULT_FILESTORE_MAX_SIZE               1000000000
#define DEFAULT_FILESTORE_MAX_WRITE_SIZE         1000000
#define DEFAULT_FILESTORE_ROLL_HOUR              1
#define DEFAULT_FILESTORE_ROLL_MINUTE            15
#define DEFAULT_BUFFERSTORE_MAX_QUEUE_LENGTH     2000000
#define DEFAULT_BUFFERSTORE_SEND_RATE            1
#define DEFAULT_BUFFERSTORE_AVG_RETRY_INTERVAL   300
#define DEFAULT_BUFFERSTORE_RETRY_INTERVAL_RANGE 60
#define DEFAULT_BUCKETSTORE_DELIMITER            ':'
#define DEFAULT_NETWORKSTORE_CACHE_TIMEOUT       300

ConnPool g_connPool;

const string meta_logfile_prefix = "scribe_meta<new_logfile>: ";

boost::shared_ptr<Store>
Store::createStore(const string& type, const string& category,
                   bool readable, bool multi_category) {
  if (0 == type.compare("file")) {
    return shared_ptr<Store>(new FileStore(category, multi_category, readable));
  } else if (0 == type.compare("buffer")) {
    return shared_ptr<Store>(new BufferStore(category, multi_category));
  } else if (0 == type.compare("network")) {
    return shared_ptr<Store>(new NetworkStore(category, multi_category));
  } else if (0 == type.compare("bucket")) {
    return shared_ptr<Store>(new BucketStore(category, multi_category));
  } else if (0 == type.compare("thriftfile")) {
    return shared_ptr<Store>(new ThriftFileStore(category, multi_category));
  } else if (0 == type.compare("null")) {
    return shared_ptr<Store>(new NullStore(category, multi_category));
  } else if (0 == type.compare("multi")) {
    return shared_ptr<Store>(new MultiStore(category, multi_category));
  } else if (0 == type.compare("category")) {
    return shared_ptr<Store>(new CategoryStore(category, multi_category));
  } else if (0 == type.compare("multifile")) {
    return shared_ptr<Store>(new MultiFileStore(category, multi_category));
  } else if (0 == type.compare("thriftmultifile")) {
    return shared_ptr<Store>(new ThriftMultiFileStore(category, multi_category));
  } else {
    return shared_ptr<Store>();
  }
}

Store::Store(const string& category, const string &type, bool multi_category)
  : categoryHandled(category),
    multiCategory(multi_category),
    storeType(type) {
  pthread_mutex_init(&statusMutex, NULL);
  LOG_OPER("[%s] Created %s store", categoryHandled.c_str(), storeType.c_str());
}

Store::~Store() {
  pthread_mutex_destroy(&statusMutex);
}

void Store::setStatus(const std::string& new_status) {
  pthread_mutex_lock(&statusMutex);
  status = new_status;
  pthread_mutex_unlock(&statusMutex);
}

std::string Store::getStatus() {
  pthread_mutex_lock(&statusMutex);
  std::string return_status(status);
  pthread_mutex_unlock(&statusMutex);
  return return_status;
}

bool Store::readOldest(/*out*/ boost::shared_ptr<logentry_vector_t> messages,
                       struct tm* now) {
  LOG_OPER("[%s] ERROR: attempting to read from a write-only store", categoryHandled.c_str());
  return false;
}

bool Store::replaceOldest(boost::shared_ptr<logentry_vector_t> messages,
                          struct tm* now) {
  LOG_OPER("[%s] ERROR: attempting to read from a write-only store", categoryHandled.c_str());
  return false;
}

void Store::deleteOldest(struct tm* now) {
   LOG_OPER("[%s] ERROR: attempting to read from a write-only store", categoryHandled.c_str());
}

bool Store::empty(struct tm* now) {
  LOG_OPER("[%s] ERROR: attempting to read from a write-only store", categoryHandled.c_str());
  return true;
}

const std::string& Store::getType() {
  return storeType;
}

FileStoreBase::FileStoreBase(const string& category, const string &type,
                             bool multi_category)
  : Store(category, type, multi_category),
    baseFilePath("/tmp"),
    subDirectory(""),
    filePath("/tmp"),
    baseFileName(category),
    baseSymlinkName(""),
    maxSize(DEFAULT_FILESTORE_MAX_SIZE),
    maxWriteSize(DEFAULT_FILESTORE_MAX_WRITE_SIZE),
    rollPeriod(ROLL_NEVER),
    rollPeriodLength(0),
    rollHour(DEFAULT_FILESTORE_ROLL_HOUR),
    rollMinute(DEFAULT_FILESTORE_ROLL_MINUTE),
    fsType("std"),
    chunkSize(0),
    writeMeta(false),
    writeCategory(false),
    createSymlink(true),
    storeTree(false),
    writeStats(true),
    lzoCompressionLevel(0),
    currentSize(0),
    lastRollTime(0),
    eventsWritten(0) {
}

FileStoreBase::~FileStoreBase() {
}

void FileStoreBase::configure(pStoreConf configuration) {

  // We can run using defaults for all of these, but there are
  // a couple of suspicious things we warn about.
  std::string tmp;
  configuration->getString("file_path", baseFilePath);
  configuration->getString("sub_directory", subDirectory);
  configuration->getString("use_hostname_sub_directory", tmp);

  if (0 == tmp.compare("yes")) {
    setHostNameSubDir();
  }

  filePath = baseFilePath;
  if (!subDirectory.empty()) {
    filePath += "/" + subDirectory;
  }


  if (!configuration->getString("base_filename", baseFileName)) {
    LOG_OPER("[%s] WARNING: Bad config - no base_filename specified for file store", categoryHandled.c_str());
  }

  // check if symlink name is optionally specified
  configuration->getString("base_symlink_name", baseSymlinkName);

  if (configuration->getString("rotate_period", tmp)) {
    if (0 == tmp.compare("hourly")) {
      rollPeriod = ROLL_HOURLY;
    } else if (0 == tmp.compare("daily")) {
      rollPeriod = ROLL_DAILY;
    } else if (0 == tmp.compare("never")) {
      rollPeriod = ROLL_NEVER;
    } else {
      errno = 0;
      char* endptr;
      rollPeriod = ROLL_OTHER;
      rollPeriodLength = strtol(tmp.c_str(), &endptr, 10);

      bool ok = errno == 0 && rollPeriodLength > 0 && endptr != tmp.c_str() &&
                (*endptr == '\0' || endptr[1] == '\0');
      switch (*endptr) {
        case 'w':
          rollPeriodLength *= 60 * 60 * 24 * 7;
          break;
        case 'd':
          rollPeriodLength *= 60 * 60 * 24;
          break;
        case 'h':
          rollPeriodLength *= 60 * 60;
          break;
        case 'm':
          rollPeriodLength *= 60;
          break;
        case 's':
        case '\0':
          break;
        default:
          ok = false;
          break;
      }

      if (!ok) {
        rollPeriod = ROLL_NEVER;
        LOG_OPER("[%s] WARNING: Bad config - invalid format of rotate_period,"
                 " rotations disabled", categoryHandled.c_str());
      }
    }
  }

  if (configuration->getString("write_meta", tmp)) {
    if (0 == tmp.compare("yes")) {
      writeMeta = true;
    }
  }
  if (configuration->getString("write_category", tmp)) {
    if (0 == tmp.compare("yes")) {
      writeCategory = true;
    }
  }

  if (configuration->getString("create_symlink", tmp)) {
    if (0 == tmp.compare("yes")) {
      createSymlink = true;
    } else {
      createSymlink = false;
    }
  }

  if (configuration->getString("use_tree", tmp)) {
    if (0 == tmp.compare("yes")) {
      // force ROLL_HOURLY if config turns on storeTree
      storeTree = true;
      rollPeriod = ROLL_HOURLY;
    } else {
      storeTree = false;
    }
  }

  if (configuration->getString("write_stats", tmp)) {
    if (0 == tmp.compare("yes")) {
      writeStats = true;
    } else {
      writeStats = false;
    }
  }

  configuration->getString("fs_type", fsType);

  configuration->getUnsigned("lzo_compression", lzoCompressionLevel);
  configuration->getUnsigned("max_size", maxSize);
  configuration->getUnsigned("max_write_size", maxWriteSize);
  configuration->getUnsigned("rotate_hour", rollHour);
  configuration->getUnsigned("rotate_minute", rollMinute);
  configuration->getUnsigned("chunk_size", chunkSize);
}

void FileStoreBase::copyCommon(const FileStoreBase *base) {
  subDirectory = base->subDirectory;
  chunkSize = base->chunkSize;
  maxSize = base->maxSize;
  maxWriteSize = base->maxWriteSize;
  rollPeriod = base->rollPeriod;
  rollPeriodLength = base->rollPeriodLength;
  rollHour = base->rollHour;
  rollMinute = base->rollMinute;
  fsType = base->fsType;
  writeMeta = base->writeMeta;
  writeCategory = base->writeCategory;
  createSymlink = base->createSymlink;
  baseSymlinkName = base->baseSymlinkName;
  storeTree = base->storeTree;
  writeStats = base->writeStats;
  lzoCompressionLevel = base->lzoCompressionLevel;

  /*
   * append the category name to the base file path and change the
   * baseFileName to the category name. these are arbitrary, could be anything
   * unique
   */
  baseFilePath = base->baseFilePath + std::string("/") + categoryHandled;
  filePath = baseFilePath;
  if (!subDirectory.empty()) {
    filePath += "/" + subDirectory;
  }

  baseFileName = categoryHandled;
}

bool FileStoreBase::open() {
  return openInternal(fsType.compare("hdfs") == 0, NULL);
}

void FileStoreBase::periodicCheck() {

  time_t rawtime = time(NULL);
  struct tm timeinfo;
  localtime_r(&rawtime, &timeinfo);

  // Roll the file if we're over max size, or an hour or day has passed
  bool rotate = ((currentSize > maxSize) && (maxSize != 0));
  if (!rotate) {
    switch (rollPeriod) {
      case ROLL_DAILY:
        rotate = timeinfo.tm_mday != lastRollTime &&
                 static_cast<uint>(timeinfo.tm_hour) >= rollHour &&
                 static_cast<uint>(timeinfo.tm_min) >= rollMinute;
        break;
      case ROLL_HOURLY:
        rotate = timeinfo.tm_hour != lastRollTime &&
                 static_cast<uint>(timeinfo.tm_min) >= rollMinute;
        break;
      case ROLL_OTHER:
        rotate = rawtime >= lastRollTime + rollPeriodLength;
        break;
      case ROLL_NEVER:
        break;
    }
  }

  if (rotate) {
    rotateFile(rawtime);
  }
}

void FileStoreBase::rotateFile(time_t currentTime) {
  struct tm timeinfo;

  currentTime = currentTime > 0 ? currentTime : time(NULL);
  localtime_r(&currentTime, &timeinfo);

  LOG_OPER("[%s] %d:%d rotating file <%s> old size <%lu> max size <%lu>",
           categoryHandled.c_str(), timeinfo.tm_hour, timeinfo.tm_min,
           makeBaseFilename(&timeinfo).c_str(), currentSize, maxSize);

  printStats();
  openInternal(true, &timeinfo);
}

string FileStoreBase::makeFullFilename(int suffix, struct tm* creation_time) {

  ostringstream filename;

  filename << filePath << '/';
  filename << makeBaseFilename(creation_time);
  filename << '_' << setw(5) << setfill('0') << suffix;

  string fullFilename = filename.str();
  if(lzoCompressionLevel > 0)
    fullFilename += ".lzo";

  return fullFilename;
}

string FileStoreBase::makeBaseSymlink() {
  ostringstream base;
  if (!baseSymlinkName.empty()) {
    base << baseSymlinkName << "_current";
  } else {
    base << baseFileName << "_current";
  }
  return base.str();
}

string FileStoreBase::makeFullSymlink() {
  ostringstream filename;
  filename << filePath << '/' << makeBaseSymlink();
  return filename.str();
}

string FileStoreBase::makeBaseFilename(struct tm* creation_time) {
  ostringstream filename;

  if (rollPeriod != ROLL_NEVER) {
    if (storeTree) {
      filename << creation_time->tm_year + 1900  << '/'
	       << setw(2) << setfill('0') << creation_time->tm_mon + 1 << '/'
	       << setw(2) << setfill('0') << creation_time->tm_mday << '/'
	       << setw(2) << setfill('0') << creation_time->tm_hour << '/';
      filename << baseFileName;
      filename << '-' << creation_time->tm_year + 1900  << '-'
	       << setw(2) << setfill('0') << creation_time->tm_mon + 1 << '-'
	       << setw(2) << setfill('0')  << creation_time->tm_mday << "-"
	       << setw(2) << setfill('0')  << creation_time->tm_hour;
    } else {
      filename << baseFileName;

      filename << '-' << creation_time->tm_year + 1900  << '-'
	       << setw(2) << setfill('0') << creation_time->tm_mon + 1 << '-'
	       << setw(2) << setfill('0')  << creation_time->tm_mday;
    }
  } else {
    filename << baseFileName;
  } 

  return filename.str();
}

// returns the suffix of the newest file matching base_filename
int FileStoreBase::findNewestFile(const string& base_filename) {

  /// do not use filePath when we are using the tree store.
  string currentPath;
  if (storeTree) { 
    string::size_type slash;
    currentPath = filePath + "/" + base_filename;
    slash = currentPath.find_last_of("/");
    currentPath = currentPath.substr(0, slash);
  } else {
    currentPath = filePath;
  }

  std::vector<std::string> files = FileInterface::list(currentPath, fsType);

  int max_suffix = -1;
  std::string retval;
  for (std::vector<std::string>::iterator iter = files.begin();
       iter != files.end();
       ++iter) {
    int suffix = getFileSuffix(*iter, base_filename);
    if (suffix > max_suffix) {
      max_suffix = suffix;
    }
  }
  return max_suffix;
}

int FileStoreBase::findOldestFile(const string& base_filename) {

  std::vector<std::string> files = FileInterface::list(filePath, fsType);

  int min_suffix = -1;
  std::string retval;
  for (std::vector<std::string>::iterator iter = files.begin();
       iter != files.end();
       ++iter) {

    int suffix = getFileSuffix(*iter, base_filename);
    if (suffix >= 0 &&
        (min_suffix == -1 || suffix < min_suffix)) {
      min_suffix = suffix;
    }
  }
  return min_suffix;
}

int FileStoreBase::getFileSuffix(const string& filename, const string& base_filename) {
  int suffix = -1;

  string mybase;
  string::size_type suffix_pos = filename.rfind('_');
  string::size_type slash;
  
  if ((slash = base_filename.find_last_of("/")) != 0) {
    mybase  = base_filename.substr(slash + 1,base_filename.length()-slash);
  } else {
    mybase  = base_filename;
  }
  
  bool retVal = (0 == filename.substr(0, suffix_pos).compare(mybase));
  
  if (string::npos != suffix_pos &&
      filename.length() > suffix_pos &&
      retVal) {
    stringstream stream;
    string::size_type lzo_suffix = filename.rfind(".lzo");

    if(lzo_suffix != string::npos) {
      stream << filename.substr(suffix_pos + 1, filename.length() - lzo_suffix+1);
    }
    else
      stream << filename.substr(suffix_pos + 1);

    stream >> suffix;
  }
  return suffix;
}

void FileStoreBase::printStats() {
  if (!writeStats) {
    return;
  }

  string filename(filePath);
  filename += "/scribe_stats";

  boost::shared_ptr<FileInterface> stats_file = FileInterface::createFileInterface(fsType, filename);
  if (!stats_file ||
      !stats_file->createDirectory(filePath) ||
      !stats_file->openWrite()) {
    LOG_OPER("[%s] Failed to open stats file <%s> of type <%s> for writing",
             categoryHandled.c_str(), filename.c_str(), fsType.c_str());
    // This isn't enough of a problem to change our status
    return;
  }

  time_t rawtime = time(NULL);
  struct tm timeinfo;
  localtime_r(&rawtime, &timeinfo);

  ostringstream msg;
  msg << timeinfo.tm_year + 1900  << '-'
      << setw(2) << setfill('0') << timeinfo.tm_mon + 1 << '-'
      << setw(2) << setfill('0') << timeinfo.tm_mday << '-'
      << setw(2) << setfill('0') << timeinfo.tm_hour << ':'
      << setw(2) << setfill('0') << timeinfo.tm_min;

  msg << " wrote <" << currentSize << "> bytes in <" << eventsWritten
      << "> events to file <" << currentFilename << ">" << endl;

  stats_file->write(msg.str());
  stats_file->close();
}

// Returns the number of bytes to pad to align to the specified chunk size
unsigned long FileStoreBase::bytesToPad(unsigned long next_message_length,
                                        unsigned long current_file_size,
                                        unsigned long chunk_size) {

  if (chunk_size > 0) {
    unsigned long space_left_in_chunk = chunk_size - current_file_size % chunk_size;
    if (next_message_length > space_left_in_chunk) {
      return space_left_in_chunk;
    } else {
      return 0;
    }
  }
  // chunk_size <= 0 means don't do any chunking
  return 0;
}

// set subDirectory to the name of this machine
void FileStoreBase::setHostNameSubDir() {
  if (!subDirectory.empty()) {
    string error_msg = "WARNING: Bad config - ";
    error_msg += "use_hostname_sub_directory will override sub_directory path";
    LOG_OPER("[%s] %s", categoryHandled.c_str(), error_msg.c_str());
  }

  char hostname[255];
  int error = gethostname(hostname, sizeof(hostname));
  if (error) {
    LOG_OPER("[%s] WARNING: gethostname returned error: %d ",
             categoryHandled.c_str(), error);
  }

  string hoststring(hostname);

  if (hoststring.empty()) {
    LOG_OPER("[%s] WARNING: could not get host name",
             categoryHandled.c_str());
  } else {
    subDirectory = hoststring;
  }
}

FileStore::FileStore(const string& category, bool multi_category,
                     bool is_buffer_file)
  : FileStoreBase(category, "file", multi_category),
    isBufferFile(is_buffer_file),
    addNewlines(false) {
}

FileStore::~FileStore() {
}

void FileStore::configure(pStoreConf configuration) {
  FileStoreBase::configure(configuration);

  // We can run using defaults for all of these, but there are
  // a couple of suspicious things we warn about.
  if (isBufferFile) {
    // scheduled file rotations of buffer files lead to too many messy cases
    rollPeriod = ROLL_NEVER;

    // Chunks don't work with the buffer file. There's no good reason
    // for this, it's just that the FileStore handles chunk padding and
    // the FileInterface handles framing, and you need to look at both to
    // read a file that's both chunked and framed. The buffer file has
    // to be framed, so we don't allow it to be chunked.
    // (framed means we write a message size to disk before the message
    //  data, which allows us to identify separate messages in binary data.
    //  Chunked means we pad with zeroes to ensure that every multiple
    //  of n bytes is the start of a message, which helps in recovering
    //  corrupted binary data and seeking into large files)
    chunkSize = 0;

    // Combine all categories in a single file for buffers
    if (multiCategory) {
      writeCategory = true;
    }
  }

  unsigned long inttemp = 0;
  configuration->getUnsigned("add_newlines", inttemp);
  addNewlines = inttemp ? true : false;
}

bool FileStore::openInternal(bool incrementFilename, struct tm* current_time) {
  bool success = false;
  struct tm timeinfo;

  if (!current_time) {
    time_t rawtime = time(NULL);
    localtime_r(&rawtime, &timeinfo);
    current_time = &timeinfo;
  }

  try {
    int suffix = findNewestFile(makeBaseFilename(current_time));

    if (incrementFilename) {
      ++suffix;
    }

    // this is the case where there's no file there and we're not incrementing
    if (suffix < 0) {
      suffix = 0;
    }

    string file = makeFullFilename(suffix, current_time);

    switch (rollPeriod) {
      case ROLL_DAILY:
        lastRollTime = current_time->tm_mday;
        break;
      case ROLL_HOURLY:
        lastRollTime = current_time->tm_hour;
        break;
      case ROLL_OTHER:
        lastRollTime = time(NULL);
        break;
      case ROLL_NEVER:
        break;
    }

    if (writeFile) {
      if (writeMeta) {
        writeFile->write(meta_logfile_prefix + file);
      }
      writeFile->close();
    }

    writeFile = FileInterface::createFileInterface(fsType, file, isBufferFile);
    if (!writeFile) {
      LOG_OPER("[%s] Failed to create file <%s> of type <%s> for writing",
               categoryHandled.c_str(), file.c_str(), fsType.c_str());
      setStatus("file open error");
      return false;
    }
    writeFile->setShouldLZOCompress(lzoCompressionLevel);

    success = writeFile->createDirectory(baseFilePath);

    // If we created a subdirectory, we need to create two directories
    if (success && !subDirectory.empty()) {
      success = writeFile->createDirectory(filePath);
    }

    if (!success) {
      LOG_OPER("[%s] Failed to create directory for file <%s>",
               categoryHandled.c_str(), file.c_str());
      setStatus("File open error");
      return false;
    }

    success = writeFile->openWrite();


    if (!success) {
      LOG_OPER("[%s] Failed to open file <%s> for writing",
              categoryHandled.c_str(),
              file.c_str());
      setStatus("File open error");
    } else {

      /* just make a best effort here, and don't error if it fails */
      if (createSymlink && !isBufferFile) {
        string symlinkName = makeFullSymlink();
        boost::shared_ptr<FileInterface> tmp =
          FileInterface::createFileInterface(fsType, symlinkName, isBufferFile);
        tmp->deleteFile();
        writeFile->createSymlink(file, symlinkName);
      }
      // else it confuses the filename code on reads

      LOG_OPER("[%s] Opened file <%s> for writing", categoryHandled.c_str(),
              file.c_str());

      currentSize = writeFile->fileSize();
      currentFilename = file;
      eventsWritten = 0;
      setStatus("");
    }

  } catch(std::exception const& e) {
    LOG_OPER("[%s] Failed to create/open file of type <%s> for writing",
             categoryHandled.c_str(), fsType.c_str());
    LOG_OPER("Exception: %s", e.what());
    setStatus("file create/open error");

    return false;
  }
  return success;
}

bool FileStore::isOpen() {
  return writeFile && writeFile->isOpen();
}

void FileStore::close() {
  if (writeFile) {
    writeFile->close();
  }
}

void FileStore::flush() {
  if (writeFile) {
    writeFile->flush();
  }
}

shared_ptr<Store> FileStore::copy(const std::string &category) {
  FileStore *store = new FileStore(category, multiCategory, isBufferFile);
  shared_ptr<Store> copied = shared_ptr<Store>(store);

  store->addNewlines = addNewlines;
  store->copyCommon(this);
  return copied;
}

bool FileStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {
  if (!isOpen()) {
    openInternal(true, NULL);
  }

  if (!isOpen()) {
    LOG_OPER("[%s] File failed to open FileStore::handleMessages()", categoryHandled.c_str());
    return false;
  }

  // write messages to current file
  return writeMessages(messages);
}

// writes messages to either the specified file or the the current writeFile
bool FileStore::writeMessages(boost::shared_ptr<logentry_vector_t> messages,
                              boost::shared_ptr<FileInterface> file) {
  // Data is written to a buffer first, then sent to disk in one call to write.
  // This costs an extra copy of the data, but dramatically improves latency with
  // network based files. (nfs, etc)
  string        write_buffer;
  bool          success = true;
  unsigned long current_size_buffered = 0; // size of data in write_buffer
  unsigned long num_buffered = 0;
  unsigned long num_written = 0;
  boost::shared_ptr<FileInterface> write_file;
  unsigned long max_write_size = min(maxSize, maxWriteSize);

  // if no file given, use current writeFile
  if (file) {
    write_file = file;
  } else {
    write_file = writeFile;
  }

  try {
    for (logentry_vector_t::iterator iter = messages->begin();
         iter != messages->end();
         ++iter) {

      // have to be careful with the length here. getFrame wants the length without
      // the frame, then bytesToPad wants the length of the frame and the message.
      unsigned long length = 0;
      unsigned long message_length = (*iter)->message.length();
      string frame, category_frame;

      if (addNewlines) {
        ++message_length;
      }

      length += message_length;

      if (writeCategory) {
        //add space for category+newline and category frame
        unsigned long category_length = (*iter)->category.length() + 1;
        length += category_length;

        category_frame = write_file->getFrame(category_length);
        length += category_frame.length();
      }

      // frame is a header that the underlying file class can add to each message
      frame = write_file->getFrame(message_length);

      length += frame.length();

      // padding to align messages on chunk boundaries
      unsigned long padding = bytesToPad(length, current_size_buffered, chunkSize);

      length += padding;

      if (padding) {
        write_buffer += string(padding, 0);
      }

      if (writeCategory) {
        write_buffer += category_frame;
        write_buffer += (*iter)->category + "\n";
      }

      write_buffer += frame;
      write_buffer += (*iter)->message;

      if (addNewlines) {
        write_buffer += "\n";
      }

      current_size_buffered += length;
      num_buffered++;

      // Write buffer if processing last message or if larger than allowed
      if ((currentSize + current_size_buffered > max_write_size && maxSize != 0) ||
          messages->end() == iter + 1 ) {
        if (!write_file->write(write_buffer)) {
          LOG_OPER("[%s] File store failed to write (%lu) messages to file",
                   categoryHandled.c_str(), messages->size());
          setStatus("File write error");
          success = false;
          break;
        }

        num_written += num_buffered;
        currentSize += current_size_buffered;
        num_buffered = 0;
        current_size_buffered = 0;
        write_buffer = "";
      }

      // rotate file if large enough and not writing to a separate file
      if ((currentSize > maxSize && maxSize != 0 )&& !file) {
        rotateFile();
        write_file = writeFile;
      }
    }
  } catch (std::exception const& e) {
    LOG_OPER("[%s] File store failed to write. Exception: %s",
             categoryHandled.c_str(), e.what());
    success = false;
  }

  eventsWritten += num_written;

  if (!success) {
    close();

    // update messages to include only the messages that were not handled
    if (num_written > 0) {
      messages->erase(messages->begin(), messages->begin() + num_written);
    }
  }

  return success;
}

void FileStore::deleteOldest(struct tm* now) {

  int index = findOldestFile(makeBaseFilename(now));
  if (index < 0) {
    return;
  }
  shared_ptr<FileInterface> deletefile = FileInterface::createFileInterface(fsType,
                                            makeFullFilename(index, now));
  deletefile->deleteFile();
}

// Replace the messages in the oldest file at this timestamp with the input messages
bool FileStore::replaceOldest(boost::shared_ptr<logentry_vector_t> messages,
                              struct tm* now) {
  string base_name = makeBaseFilename(now);
  int index = findOldestFile(base_name);
  if (index < 0) {
    LOG_OPER("[%s] Could not find files <%s>", categoryHandled.c_str(), base_name.c_str());
    return false;
  }

  string filename = makeFullFilename(index, now);

  // Need to close and reopen store in case we already have this file open
  close();

  shared_ptr<FileInterface> infile = FileInterface::createFileInterface(fsType,
                                          filename, isBufferFile);

  // overwrite the old contents of the file
  bool success;
  if (infile->openTruncate()) {
    success = writeMessages(messages, infile);

  } else {
    LOG_OPER("[%s] Failed to open file <%s> for writing and truncate",
             categoryHandled.c_str(), filename.c_str());
    success = false;
  }

  // close this file and re-open store
  infile->close();
  open();

  return success;
}

bool FileStore::readOldest(/*out*/ boost::shared_ptr<logentry_vector_t> messages,
                           struct tm* now) {

  int index = findOldestFile(makeBaseFilename(now));
  if (index < 0) {
    // This isn't an error. It's legit to call readOldest when there aren't any
    // files left, in which case the call succeeds but returns messages empty.
    return true;
  }
  std::string filename = makeFullFilename(index, now);

  shared_ptr<FileInterface> infile = FileInterface::createFileInterface(fsType, filename, isBufferFile);

  if (!infile->openRead()) {
    LOG_OPER("[%s] Failed to open file <%s> for reading", categoryHandled.c_str(), filename.c_str());
    return false;
  }

  std::string message;
  while (infile->readNext(message)) {
    if (!message.empty()) {
      logentry_ptr_t entry = logentry_ptr_t(new LogEntry);

      // check whether a category is stored with the message
      if (writeCategory) {
        // get category without trailing \n
        entry->category = message.substr(0, message.length() - 1);

        if (!infile->readNext(message)) {
          LOG_OPER("[%s] category not stored with message <%s>",
                   categoryHandled.c_str(), entry->category.c_str());
        }
      } else {
        entry->category = categoryHandled;
      }

      entry->message = message;

      messages->push_back(entry);
    }
  }
  infile->close();

  LOG_OPER("[%s] successfully read <%lu> entries from file <%s>",
        categoryHandled.c_str(), messages->size(), filename.c_str());
  return true;
}

bool FileStore::empty(struct tm* now) {

  std::vector<std::string> files = FileInterface::list(filePath, fsType);

  std::string base_filename = makeBaseFilename(now);
  for (std::vector<std::string>::iterator iter = files.begin();
       iter != files.end();
       ++iter) {
    int suffix =  getFileSuffix(*iter, base_filename);
    if (-1 != suffix) {
      std::string fullname = makeFullFilename(suffix, now);
      shared_ptr<FileInterface> file = FileInterface::createFileInterface(fsType, fullname);
      if (file->fileSize()) {
        return false;
      }
    } // else it doesn't match the filename for this store
  }
  return true;
}


ThriftFileStore::ThriftFileStore(const std::string& category, bool multi_category)
  : FileStoreBase(category, "thriftfile", multi_category),
    flushFrequencyMs(0),
    msgBufferSize(0),
    useSimpleFile(0) {
}

ThriftFileStore::~ThriftFileStore() {
}

shared_ptr<Store> ThriftFileStore::copy(const std::string &category) {
  ThriftFileStore *store = new ThriftFileStore(category, multiCategory);
  shared_ptr<Store> copied = shared_ptr<Store>(store);

  store->flushFrequencyMs = flushFrequencyMs;
  store->msgBufferSize = msgBufferSize;
  store->copyCommon(this);
  return copied;
}

bool ThriftFileStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {
  if (!isOpen()) {
    return false;
  }

  unsigned long messages_handled = 0;
  for (logentry_vector_t::iterator iter = messages->begin();
       iter != messages->end();
       ++iter) {

    // This length is an estimate -- what the ThriftLogFile actually writes is a black box to us
    uint32_t length = (*iter)->message.size();

    try {
      thriftFileTransport->write(reinterpret_cast<const uint8_t*>((*iter)->message.data()), length);
      currentSize += length;
      ++eventsWritten;
      ++messages_handled;
    } catch (TException te) {
      LOG_OPER("[%s] Thrift file store failed to write to file: %s\n", categoryHandled.c_str(), te.what());
      setStatus("File write error");

      // If we already handled some messages, remove them from vector before
      // returning failure
      if (messages_handled) {
        messages->erase(messages->begin(), iter);
      }
      return false;
    }
  }
  // We can't wait until periodicCheck because we could be getting
  // a lot of data all at once in a failover situation
  if (currentSize > maxSize && maxSize != 0) {
    rotateFile();
  }

  return true;
}

bool ThriftFileStore::open() {
  return openInternal(true, NULL);
}

bool ThriftFileStore::isOpen() {
  return thriftFileTransport && thriftFileTransport->isOpen();
}

void ThriftFileStore::configure(pStoreConf configuration) {
  FileStoreBase::configure(configuration);
  configuration->getUnsigned("flush_frequency_ms", flushFrequencyMs);
  configuration->getUnsigned("msg_buffer_size", msgBufferSize);
  configuration->getUnsigned("use_simple_file", useSimpleFile);
}

void ThriftFileStore::close() {
  thriftFileTransport.reset();
}

void ThriftFileStore::flush() {
  // TFileTransport has its own periodic flushing mechanism, and we
  // introduce deadlocks if we try to call it from more than one place
  return;
}

bool ThriftFileStore::openInternal(bool incrementFilename, struct tm* current_time) {
  struct tm timeinfo;

  if (!current_time) {
    time_t rawtime = time(NULL);
    localtime_r(&rawtime, &timeinfo);
    current_time = &timeinfo;
  }

  int suffix = findNewestFile(makeBaseFilename(current_time));

  if (incrementFilename) {
    ++suffix;
  }

  // this is the case where there's no file there and we're not incrementing
  if (suffix < 0) {
    suffix = 0;
  }

  string filename = makeFullFilename(suffix, current_time);
  /* try to create the directory containing the file */
  if (!createFileDirectory()) {
    LOG_OPER("[%s] Could not create path for file: %s",
             categoryHandled.c_str(), filename.c_str());
    return false;
  }

  switch (rollPeriod) {
    case ROLL_DAILY:
      lastRollTime = current_time->tm_mday;
      break;
    case ROLL_HOURLY:
      lastRollTime = current_time->tm_hour;
      break;
    case ROLL_OTHER:
      lastRollTime = time(NULL);
      break;
    case ROLL_NEVER:
      break;
  }


  try {
    if (useSimpleFile) {
      thriftFileTransport.reset(new TSimpleFileTransport(filename, false, true));
    } else {
      TFileTransport *transport = new TFileTransport(filename);
      thriftFileTransport.reset(transport);

      if (chunkSize != 0) {
	transport->setChunkSize(chunkSize);
      }
      if (flushFrequencyMs > 0) {
	transport->setFlushMaxUs(flushFrequencyMs * 1000);
      }
      if (msgBufferSize > 0) {
	transport->setEventBufferSize(msgBufferSize);
      }
    }

    LOG_OPER("[%s] Opened file <%s> for writing", categoryHandled.c_str(), filename.c_str());

    struct stat st;
    if (stat(filename.c_str(), &st) == 0) {
      currentSize = st.st_size;
    } else {
      currentSize = 0;
    }
    currentFilename = filename;
    eventsWritten = 0;
    setStatus("");
  } catch (TException te) {
    LOG_OPER("[%s] Failed to open file <%s> for writing: %s\n", categoryHandled.c_str(), filename.c_str(), te.what());
    setStatus("File open error");
    return false;
  }

  /* just make a best effort here, and don't error if it fails */
  if (createSymlink) {
    string symlinkName = makeFullSymlink();
    unlink(symlinkName.c_str());
    symlink(filename.c_str(), symlinkName.c_str());
  }

  return true;
}

bool ThriftFileStore::createFileDirectory () {
  try {
    boost::filesystem::create_directory(baseFilePath);

    // If we created a subdirectory, we need to create two directories
    if (!subDirectory.empty()) {
      boost::filesystem::create_directory(filePath);
    }
  }catch(std::exception const& e) {
    LOG_OPER("Exception < %s > trying to create directory", e.what());
    return false;
  }
  return true;
}

BufferStore::BufferStore(const string& category, bool multi_category)
  : Store(category, "buffer", multi_category),
    maxQueueLength(DEFAULT_BUFFERSTORE_MAX_QUEUE_LENGTH),
    bufferSendRate(DEFAULT_BUFFERSTORE_SEND_RATE),
    avgRetryInterval(DEFAULT_BUFFERSTORE_AVG_RETRY_INTERVAL),
    retryIntervalRange(DEFAULT_BUFFERSTORE_RETRY_INTERVAL_RANGE),
    replayBuffer(true),
    state(DISCONNECTED) {

  lastWriteTime = lastOpenAttempt = time(NULL);
  retryInterval = getNewRetryInterval();

  // we can't open the client conection until we get configured
}

BufferStore::~BufferStore() {

}

void BufferStore::configure(pStoreConf configuration) {

  // Constructor defaults are fine if these don't exist
  configuration->getUnsigned("max_queue_length", (unsigned long&) maxQueueLength);
  configuration->getUnsigned("buffer_send_rate", (unsigned long&) bufferSendRate);
  configuration->getUnsigned("retry_interval", (unsigned long&) avgRetryInterval);
  configuration->getUnsigned("retry_interval_range", (unsigned long&) retryIntervalRange);

  string tmp;
  if (configuration->getString("replay_buffer", tmp) && tmp != "yes") {
    replayBuffer = false;
  }

  if (retryIntervalRange > avgRetryInterval) {
    LOG_OPER("[%s] Bad config - retry_interval_range must be less than retry_interval. Using <%d> as range instead of <%d>",
             categoryHandled.c_str(), (int)avgRetryInterval, (int)retryIntervalRange);
    retryIntervalRange = avgRetryInterval;
  }

  pStoreConf secondary_store_conf;
  if (!configuration->getStore("secondary", secondary_store_conf)) {
    string msg("Bad config - buffer store doesn't have secondary store");
    setStatus(msg);
    cout << msg << endl;
  } else {
    string type;
    if (!secondary_store_conf->getString("type", type)) {
      string msg("Bad config - buffer secondary store doesn't have a type");
      setStatus(msg);
      cout << msg << endl;
    } else {
      // If replayBuffer is true, then we need to create a readable store
      secondaryStore = createStore(type, categoryHandled, replayBuffer,
                                   multiCategory);
      secondaryStore->configure(secondary_store_conf);
    }
  }

  pStoreConf primary_store_conf;
  if (!configuration->getStore("primary", primary_store_conf)) {
    string msg("Bad config - buffer store doesn't have primary store");
    setStatus(msg);
    cout << msg << endl;
  } else {
    string type;
    if (!primary_store_conf->getString("type", type)) {
      string msg("Bad config - buffer primary store doesn't have a type");
      setStatus(msg);
      cout << msg << endl;
    } else if (0 == type.compare("multi")) {
      // Cannot allow multistores in bufferstores as they can partially fail to
      // handle a message.  We cannot retry sending a messages that was
      // already handled by a subset of stores in the multistore.
      string msg("Bad config - buffer primary store cannot be multistore");
      setStatus(msg);
    } else {
      primaryStore = createStore(type, categoryHandled, false, multiCategory);
      primaryStore->configure(primary_store_conf);
    }
  }

  // If the config is bad we'll still try to write the data to a
  // default location on local disk.
  if (!secondaryStore) {
    secondaryStore = createStore("file", categoryHandled, true, multiCategory);
  }
  if (!primaryStore) {
    primaryStore = createStore("file", categoryHandled, false, multiCategory);
  }
}

bool BufferStore::isOpen() {
  return primaryStore->isOpen() || secondaryStore->isOpen();
}

bool BufferStore::open() {

  // try to open the primary store, and set the state accordingly
  if (primaryStore->open()) {
    // in case there are files left over from a previous instance
    changeState(SENDING_BUFFER);

    // If we don't need to send buffers, skip to streaming
    if (!replayBuffer) {
      // We still switch state to SENDING_BUFFER first just to make sure we
      // can open the secondary store
      changeState(STREAMING);
    }
  } else {
    secondaryStore->open();
    changeState(DISCONNECTED);
  }

  return isOpen();
}

void BufferStore::close() {
  if (primaryStore->isOpen()) {
    primaryStore->flush();
    primaryStore->close();
  }
  if (secondaryStore->isOpen()) {
    secondaryStore->flush();
    secondaryStore->close();
  }
}

void BufferStore::flush() {
  if (primaryStore->isOpen()) {
    primaryStore->flush();
  }
  if (secondaryStore->isOpen()) {
    secondaryStore->flush();
  }
}

shared_ptr<Store> BufferStore::copy(const std::string &category) {
  BufferStore *store = new BufferStore(category, multiCategory);
  shared_ptr<Store> copied = shared_ptr<Store>(store);

  store->maxQueueLength = maxQueueLength;
  store->bufferSendRate = bufferSendRate;
  store->avgRetryInterval = avgRetryInterval;
  store->retryIntervalRange = retryIntervalRange;
  store->replayBuffer = replayBuffer;

  store->primaryStore = primaryStore->copy(category);
  store->secondaryStore = secondaryStore->copy(category);
  return copied;
}

bool BufferStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {
  lastWriteTime = time(NULL);

  // If the queue is really long it's probably because the primary store isn't moving
  // fast enough and is backing up, in which case it's best to give up on it for now.
  if (state == STREAMING && messages->size() > maxQueueLength) {
    LOG_OPER("[%s] BufferStore queue backing up, switching to secondary store (%u messages)", categoryHandled.c_str(), (unsigned)messages->size());
    changeState(DISCONNECTED);
  }

  if (state == STREAMING) {
    if (primaryStore->handleMessages(messages)) {
      return true;
    } else {
      changeState(DISCONNECTED);
    }
  }

  if (state != STREAMING) {
    // If this fails there's nothing else we can do here.
    return secondaryStore->handleMessages(messages);
  }

  return false;
}

// handles entry and exit conditions for states
void BufferStore::changeState(buffer_state_t new_state) {

  // leaving this state
  switch (state) {
  case STREAMING:
    secondaryStore->open();
    break;
  case DISCONNECTED:
    // Assume that if we are now able to leave the disconnected state, any
    // former warning has now been fixed.
    setStatus("");
    break;
  case SENDING_BUFFER:
    break;
  default:
    break;
  }

  // entering this state
  switch (new_state) {
  case STREAMING:
    if (secondaryStore->isOpen()) {
      secondaryStore->close();
    }
    break;
  case DISCONNECTED:
    // Do not set status here as it is possible to be in this frequently.
    // Whatever caused us to enter this state should have either set status
    // or chosen not to set status.
    incCounter(categoryHandled, "retries");
    lastOpenAttempt = time(NULL);
    retryInterval = getNewRetryInterval();
    LOG_OPER("[%s] choosing new retry interval <%d> seconds", categoryHandled.c_str(),
             (int)retryInterval);
    if (!secondaryStore->isOpen()) {
      secondaryStore->open();
    }
    break;
  case SENDING_BUFFER:
    if (!secondaryStore->isOpen()) {
      secondaryStore->open();
    }
    break;
  default:
    break;
  }

  LOG_OPER("[%s] Changing state from <%s> to <%s>", categoryHandled.c_str(), stateAsString(state), stateAsString(new_state));
  state = new_state;
}

void BufferStore::periodicCheck() {

  // This class is responsible for checking its children
  primaryStore->periodicCheck();
  secondaryStore->periodicCheck();

  time_t now = time(NULL);
  struct tm nowinfo;
  localtime_r(&now, &nowinfo);

  if (state == DISCONNECTED) {

    if (now - lastOpenAttempt > retryInterval) {

      if (primaryStore->open()) {
        // Success.  Check if we need to send buffers from secondary to primary
        if (replayBuffer) {
          changeState(SENDING_BUFFER);
        } else {
          changeState(STREAMING);
        }
      } else {
        // this resets the retry timer
        changeState(DISCONNECTED);
      }
    }
  }

  if (state == SENDING_BUFFER) {

    // Read a group of messages from the secondary store and send them to
    // the primary store. Note that the primary store could tell us to try
    // again later, so this isn't very efficient if it reads too many
    // messages at once. (if the secondary store is a file, the number of
    // messages read is controlled by the max file size)
    unsigned sent = 0;
    for (sent = 0; sent < bufferSendRate; ++sent) {
      boost::shared_ptr<logentry_vector_t> messages(new logentry_vector_t);
      if (secondaryStore->readOldest(messages, &nowinfo)) {
        lastWriteTime = time(NULL);

        unsigned long size = messages->size();
        if (size) {
          if (primaryStore->handleMessages(messages)) {
            secondaryStore->deleteOldest(&nowinfo);
          } else {

            if (messages->size() != size) {
              // We were only able to process some, but not all of this batch
              // of messages.  Replace this batch of messages with just the messages
              // that were not processed.
              LOG_OPER("[%s] buffer store primary store processed %lu/%lu messages",
                       categoryHandled.c_str(), size - messages->size(), size);

              // Put back un-handled messages
              if (!secondaryStore->replaceOldest(messages, &nowinfo)) {
                // Nothing we can do but try to remove oldest messages and report a loss
                LOG_OPER("[%s] buffer store secondary store lost %lu messages",
                         categoryHandled.c_str(), messages->size());
                incCounter(categoryHandled, "lost", messages->size());
                secondaryStore->deleteOldest(&nowinfo);
              }
            }

            changeState(DISCONNECTED);
            break;
          }
        }  else {
          // else it's valid for read to not find anything but not error
          secondaryStore->deleteOldest(&nowinfo);
        }
      } else {
        // This is bad news. We'll stay in the sending state and keep trying to read.
        setStatus("Failed to read from secondary store");
        LOG_OPER("[%s] WARNING: buffer store can't read from secondary store", categoryHandled.c_str());
        break;
      }

      if (secondaryStore->empty(&nowinfo)) {
        LOG_OPER("[%s] No more buffer files to send, switching to streaming mode", categoryHandled.c_str());
        changeState(STREAMING);

        primaryStore->flush();
        break;
      }
    }
  }// if state == SENDING_BUFFER
}


time_t BufferStore::getNewRetryInterval() {
  time_t interval = avgRetryInterval - retryIntervalRange/2 + rand() % retryIntervalRange;
  return interval;
}

const char* BufferStore::stateAsString(buffer_state_t state) {
switch (state) {
  case STREAMING:
    return "STREAMING";
  case DISCONNECTED:
    return "DISCONNECTED";
  case SENDING_BUFFER:
    return "SENDING_BUFFER";
  default:
    return "unknown state";
  }
}

std::string BufferStore::getStatus() {

  // This order is intended to give precedence to the errors
  // that are likely to be the worst. We can handle a problem
  // with the primary store, but not the secondary.
  std::string return_status = secondaryStore->getStatus();
  if (return_status.empty()) {
    return_status = Store::getStatus();
  }
  if (return_status.empty()) {
    return_status = primaryStore->getStatus();
  }
  return return_status;
}


NetworkStore::NetworkStore(const string& category, bool multi_category)
  : Store(category, "network", multi_category),
    useConnPool(false),
    smcBased(false),
    remotePort(0),
    serviceCacheTimeout(DEFAULT_NETWORKSTORE_CACHE_TIMEOUT),
    lastServiceCheck(0),
    opened(false) {
  // we can't open the connection until we get configured

  // the bool for opened ensures that we don't make duplicate
  // close calls, which would screw up the connection pool's
  // reference counting.
}

NetworkStore::~NetworkStore() {
  close();
}

void NetworkStore::configure(pStoreConf configuration) {
  // Error checking is done on open()
  // smc takes precedence over host + port
  if (configuration->getString("smc_service", smcService)) {
    smcBased = true;

    // Constructor defaults are fine if these don't exist
    configuration->getString("service_options", serviceOptions);
    configuration->getUnsigned("service_cache_timeout", serviceCacheTimeout);
  } else {
    smcBased = false;
    configuration->getString("remote_host", remoteHost);
    configuration->getUnsigned("remote_port", remotePort);
#ifdef USE_ZOOKEEPER
    if (0 == remoteHost.find("zk://")) {
      string parentZnode = remoteHost.substr(5, string::npos);
      g_ZKClient->getRemoteScribe(parentZnode, remoteHost, remotePort);
    }
#endif
  }

  if (!configuration->getInt("timeout", timeout)) {
    timeout = DEFAULT_SOCKET_TIMEOUT_MS;
  }

  string temp;
  if (configuration->getString("use_conn_pool", temp)) {
    if (0 == temp.compare("yes")) {
      useConnPool = true;
    }
  }
}

bool NetworkStore::open() {

  if (smcBased) {
    bool success = true;
    time_t now = time(NULL);

    // Only get list of servers if we haven't already gotten them recently
    if (lastServiceCheck <= (time_t) (now - serviceCacheTimeout)) {
      lastServiceCheck = now;

      success =
        network_config::getService(smcService, serviceOptions, servers);
    }

    // Cannot open if we couldn't find any servers
    if (!success || servers.empty()) {
      LOG_OPER("[%s] Failed to get servers from smc", categoryHandled.c_str());
      setStatus("Could not get list of servers from smc");
      return false;
    }

    if (useConnPool) {
      opened = g_connPool.open(smcService, servers, static_cast<int>(timeout));
    } else {
      // only open unpooled connection if not already open
      if (unpooledConn == NULL) {
        unpooledConn = shared_ptr<scribeConn>(new scribeConn(smcService, servers, static_cast<int>(timeout)));
        opened = unpooledConn->open();
      } else {
        opened = unpooledConn->isOpen();
        if (!opened) {
          opened = unpooledConn->open();
        }
      }
    }

  } else if (remotePort <= 0 ||
             remoteHost.empty()) {
    LOG_OPER("[%s] Bad config - won't attempt to connect to <%s:%lu>", categoryHandled.c_str(), remoteHost.c_str(), remotePort);
    setStatus("Bad config - invalid location for remote server");
    return false;

  } else {
    if (useConnPool) {
      opened = g_connPool.open(remoteHost, remotePort, static_cast<int>(timeout));
    } else {
      // only open unpooled connection if not already open
      if (unpooledConn == NULL) {
        unpooledConn = shared_ptr<scribeConn>(new scribeConn(remoteHost, remotePort, static_cast<int>(timeout)));
        opened = unpooledConn->open();
      } else {
        opened = unpooledConn->isOpen();
        if (!opened) {
          opened = unpooledConn->open();
        }
      }
    }
  }


  if (opened) {
    setStatus("");
  } else {
    setStatus("Failed to connect");
  }
  return opened;
}

void NetworkStore::close() {
  if (!opened) {
    return;
  }
  opened = false;
  if (useConnPool) {
    if (smcBased) {
      g_connPool.close(smcService);
    } else {
      g_connPool.close(remoteHost, remotePort);
    }
  } else {
    if (unpooledConn != NULL) {
      unpooledConn->close();
    }
  }
}

bool NetworkStore::isOpen() {
  return opened;
}

shared_ptr<Store> NetworkStore::copy(const std::string &category) {
  NetworkStore *store = new NetworkStore(category, multiCategory);
  shared_ptr<Store> copied = shared_ptr<Store>(store);

  store->useConnPool = useConnPool;
  store->smcBased = smcBased;
  store->timeout = timeout;
  store->remoteHost = remoteHost;
  store->remotePort = remotePort;
  store->smcService = smcService;

  return copied;
}

bool NetworkStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {
  if (!isOpen()) {
    LOG_OPER("[%s] Logic error: NetworkStore::handleMessages called on closed store", categoryHandled.c_str());
    return false;
  } else if (useConnPool) {
    if (smcBased) {
      return g_connPool.send(smcService, messages);
    } else {
      return g_connPool.send(remoteHost, remotePort, messages);
    }
  } else {
    if (unpooledConn) {
      return unpooledConn->send(messages);
    } else {
      LOG_OPER("[%s] Logic error: NetworkStore::handleMessages unpooledConn is NULL", categoryHandled.c_str());
      return false;
    }
  }
}

void NetworkStore::flush() {
  // Nothing to do
}

BucketStore::BucketStore(const string& category, bool multi_category)
  : Store(category, "bucket", multi_category),
    bucketType(context_log),
    delimiter(DEFAULT_BUCKETSTORE_DELIMITER),
    removeKey(false),
    opened(false),
    bucketRange(0),
    numBuckets(1) {
}

BucketStore::~BucketStore() {

}

// Given a single bucket definition, create multiple buckets
void BucketStore::createBucketsFromBucket(pStoreConf configuration,
					  pStoreConf bucket_conf) {
  string error_msg, bucket_subdir, type, path, failure_bucket;
  bool needs_bucket_subdir = false;
  unsigned long bucket_offset = 0;
  pStoreConf tmp;

  // check for extra bucket definitions
  if (configuration->getStore("bucket0", tmp) ||
      configuration->getStore("bucket1", tmp)) {
    error_msg = "bucket store has too many buckets defined";
    goto handle_error;
  }

  bucket_conf->getString("type", type);
  if (type != "file" && type != "thriftfile") {
    error_msg = "store contained in a bucket store must have a type of ";
    error_msg += "either file or thriftfile if not defined explicitely";
    goto handle_error;
  }

  needs_bucket_subdir = true;
  if (!configuration->getString("bucket_subdir", bucket_subdir)) {
    error_msg =
      "bucketizer containing file stores must have a bucket_subdir";
    goto handle_error;
  }
  if (!bucket_conf->getString("file_path", path)) {
    error_msg =
      "file store contained by bucketizer must have a file_path";
    goto handle_error;
  }

  // set starting bucket number if specified
  configuration->getUnsigned("bucket_offset", bucket_offset);

  // check if failure bucket was given a different name
  configuration->getString("failure_bucket", failure_bucket);

  // We actually create numBuckets + 1 stores. Messages are normally
  // hashed into buckets 1 through numBuckets, and messages that can't
  // be hashed are put in bucket 0.

  for (unsigned int i = 0; i <= numBuckets; ++i) {

    shared_ptr<Store> newstore =
      createStore(type, categoryHandled, false, multiCategory);

    if (!newstore) {
      error_msg = "can't create store of type: ";
      error_msg += type;
      goto handle_error;
    }

    // For file/thrift file buckets, create unique filepath for each bucket
    if (needs_bucket_subdir) {
      if (i == 0 && !failure_bucket.empty()) {
        bucket_conf->setString("file_path", path + '/' + failure_bucket);
      } else {
        // the bucket number is appended to the file path
        unsigned int bucket_id = i + bucket_offset;

        ostringstream oss;
        oss << path << '/' << bucket_subdir << setw(3) << setfill('0')
            << bucket_id;
        bucket_conf->setString("file_path", oss.str());
      }
    }

    buckets.push_back(newstore);
    newstore->configure(bucket_conf);
  }

  return;

handle_error:
  setStatus(error_msg);
  LOG_OPER("[%s] Bad config - %s", categoryHandled.c_str(),
           error_msg.c_str());
  numBuckets = 0;
  buckets.clear();
}

// Checks for a bucket definition for every bucket from 0 to numBuckets
// and configures each bucket
void BucketStore::createBuckets(pStoreConf configuration) {
  string error_msg, tmp_string;
  pStoreConf tmp;
  unsigned long i;

  if (configuration->getString("bucket_subdir", tmp_string)) {
    error_msg =
      "cannot have bucket_subdir when defining multiple buckets";
      goto handle_error;
  }

  if (configuration->getString("bucket_offset", tmp_string)) {
    error_msg =
      "cannot have bucket_offset when defining multiple buckets";
      goto handle_error;
  }

  if (configuration->getString("failure_bucket", tmp_string)) {
    error_msg =
      "cannot have failure_bucket when defining multiple buckets";
      goto handle_error;
  }

  // Configure stores named 'bucket0, bucket1, bucket2, ... bucket{numBuckets}
  for (i = 0; i <= numBuckets; i++) {
    pStoreConf   bucket_conf;
    string       type, bucket_name;
    stringstream ss;

    ss << "bucket" << i;
    bucket_name = ss.str();

    if (!configuration->getStore(bucket_name, bucket_conf)) {
      error_msg = "could not find bucket definition for " +
	bucket_name;
      goto handle_error;
    }

    if (!bucket_conf->getString("type", type)) {
      error_msg =
	"store contained in a bucket store must have a type";
      goto handle_error;
    }

    shared_ptr<Store> bucket =
      createStore(type, categoryHandled, false, multiCategory);

    buckets.push_back(bucket);
    bucket->configure(bucket_conf);
  }

  // Check if an extra bucket is defined
  if (configuration->getStore("bucket" + (numBuckets + 1), tmp)) {
    error_msg = "bucket store has too many buckets defined";
    goto handle_error;
  }

  return;

handle_error:
  setStatus(error_msg);
  LOG_OPER("[%s] Bad config - %s", categoryHandled.c_str(),
           error_msg.c_str());
  numBuckets = 0;
  buckets.clear();
}

/**
   * Buckets in a bucket store can be defined explicitly or implicitly:
   *
   * #Explicitly
   * <store>
   *   type=bucket
   *   num_buckets=2
   *   bucket_type=key_hash
   *
   *   <bucket0>
   *     ...
   *   </bucket0>
   *
   *   <bucket1>
   *     ...
   *   </bucket1>
   *
   *   <bucket2>
   *     ...
   *   </bucket2>
   * </store>
   *
   * #Implicitly
   * <store>
   *   type=bucket
   *   num_buckets=2
   *   bucket_type=key_hash
   *
   *   <bucket>
   *     ...
   *   </bucket>
   * </store>
   */
void BucketStore::configure(pStoreConf configuration) {

  string error_msg, bucketizer_str, remove_key_str;
  unsigned long delim_long = 0;
  pStoreConf bucket_conf;
  //set this to true for bucket types that have a delimiter
  bool need_delimiter = false;

  configuration->getString("bucket_type", bucketizer_str);

  // Figure out th bucket type from the bucketizer string
  if (0 == bucketizer_str.compare("context_log")) {
    bucketType = context_log;
  } else if (0 == bucketizer_str.compare("random")) {
      bucketType = random;
  } else if (0 == bucketizer_str.compare("key_hash")) {
    bucketType = key_hash;
    need_delimiter = true;
  } else if (0 == bucketizer_str.compare("key_modulo")) {
    bucketType = key_modulo;
    need_delimiter = true;
  } else if (0 == bucketizer_str.compare("key_range")) {
    bucketType = key_range;
    need_delimiter = true;
    configuration->getUnsigned("bucket_range", bucketRange);

    if (bucketRange == 0) {
      LOG_OPER("[%s] config warning - bucket_range is 0",
               categoryHandled.c_str());
    }
  }

  // This is either a key_hash or key_modulo, not context log, figure out the delimiter and store it
  if (need_delimiter) {
    configuration->getUnsigned("delimiter", delim_long);
    if (delim_long > 255) {
      LOG_OPER("[%s] config warning - delimiter is too large to fit in a char, using default", categoryHandled.c_str());
      delimiter = DEFAULT_BUCKETSTORE_DELIMITER;
    } else if (delim_long == 0) {
      LOG_OPER("[%s] config warning - delimiter is zero, using default", categoryHandled.c_str());
      delimiter = DEFAULT_BUCKETSTORE_DELIMITER;
    } else {
      delimiter = (char)delim_long;
    }
  }

  // Optionally remove the key and delimiter of each message before bucketizing
  configuration->getString("remove_key", remove_key_str);
  if (remove_key_str == "yes") {
    removeKey = true;

    if (bucketType == context_log) {
      error_msg =
        "Bad config - bucketizer store of type context_log do not support remove_key";
      goto handle_error;
    }
  }

  if (!configuration->getUnsigned("num_buckets", numBuckets)) {
    error_msg = "Bad config - bucket store must have num_buckets";
    goto handle_error;
  }

  // Buckets can be defined explicitely or by specifying a single "bucket"
  if (configuration->getStore("bucket", bucket_conf)) {
    createBucketsFromBucket(configuration, bucket_conf);
  } else {
    createBuckets(configuration);
  }

  return;

handle_error:
  setStatus(error_msg);
  LOG_OPER("[%s] %s", categoryHandled.c_str(), error_msg.c_str());
  numBuckets = 0;
  buckets.clear();
}

bool BucketStore::open() {
  // we have one extra bucket for messages we can't hash
  if (numBuckets <= 0 || buckets.size() != numBuckets + 1) {
    LOG_OPER("[%s] Can't open bucket store with <%d> of <%lu> buckets", categoryHandled.c_str(), (int)buckets.size(), numBuckets);
    return false;
  }

  for (std::vector<shared_ptr<Store> >::iterator iter = buckets.begin();
       iter != buckets.end();
       ++iter) {

    if (!(*iter)->open()) {
      close();
      opened = false;
      return false;
    }
  }
  opened = true;
  return true;
}

bool BucketStore::isOpen() {
  return opened;
}

void BucketStore::close() {
  // don't check opened, because we can call this when some, but
  // not all, contained stores are opened. Calling close on a contained
  // store that's already closed shouldn't hurt anything.
  for (std::vector<shared_ptr<Store> >::iterator iter = buckets.begin();
       iter != buckets.end();
       ++iter) {
    (*iter)->close();
  }
  opened = false;
}

void BucketStore::flush() {
  for (std::vector<shared_ptr<Store> >::iterator iter = buckets.begin();
       iter != buckets.end();
       ++iter) {
    (*iter)->flush();
  }
}

string BucketStore::getStatus() {

  string retval = Store::getStatus();

  std::vector<shared_ptr<Store> >::iterator iter = buckets.begin();
  while (retval.empty() && iter != buckets.end()) {
    retval = (*iter)->getStatus();
    ++iter;
  }
  return retval;
}

void BucketStore::periodicCheck() {
  for (std::vector<shared_ptr<Store> >::iterator iter = buckets.begin();
       iter != buckets.end();
       ++iter) {
    (*iter)->periodicCheck();
  }
}

shared_ptr<Store> BucketStore::copy(const std::string &category) {
  BucketStore *store = new BucketStore(category, multiCategory);
  shared_ptr<Store> copied = shared_ptr<Store>(store);

  store->numBuckets = numBuckets;
  store->bucketType = bucketType;
  store->delimiter = delimiter;

  for (std::vector<shared_ptr<Store> >::iterator iter = buckets.begin();
       iter != buckets.end();
       ++iter) {
    store->buckets.push_back((*iter)->copy(category));
  }

  return copied;
}

bool BucketStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {
  bool success = true;

  boost::shared_ptr<logentry_vector_t> failed_messages(new logentry_vector_t);
  vector<shared_ptr<logentry_vector_t> > bucketed_messages;
  bucketed_messages.resize(numBuckets + 1);

  if (numBuckets == 0) {
    LOG_OPER("[%s] Failed to write - no buckets configured",
             categoryHandled.c_str());
    setStatus("Failed write to bucket store");
    return false;
  }

  // batch messages by bucket
  for (logentry_vector_t::iterator iter = messages->begin();
       iter != messages->end();
       ++iter) {
    unsigned bucket = bucketize((*iter)->message);

    if (!bucketed_messages[bucket]) {
      bucketed_messages[bucket] =
        shared_ptr<logentry_vector_t> (new logentry_vector_t);
    }

    bucketed_messages[bucket]->push_back(*iter);
  }

  // handle all batches of messages
  for (unsigned long i = 0; i <= numBuckets; i++) {
    shared_ptr<logentry_vector_t> batch = bucketed_messages[i];

    if (batch) {

      if (removeKey) {
        // Create new set of messages with keys removed
        shared_ptr<logentry_vector_t> key_removed =
          shared_ptr<logentry_vector_t> (new logentry_vector_t);

        for (logentry_vector_t::iterator iter = batch->begin();
             iter != batch->end();
             ++iter) {
          logentry_ptr_t entry = logentry_ptr_t(new LogEntry);
          entry->category = (*iter)->category;
          entry->message = getMessageWithoutKey((*iter)->message);
          key_removed->push_back(entry);
        }
        batch = key_removed;
      }

      if (!buckets[i]->handleMessages(batch)) {
        // keep track of messages that were not handled
        failed_messages->insert(failed_messages->end(),
                                bucketed_messages[i]->begin(),
                                bucketed_messages[i]->end());
        success = false;
      }
    }
  }

  if (!success) {
    // return failed logentrys in messages
    messages->swap(*failed_messages);
  }

  return success;
}

unsigned long BucketStore::bucketize(const std::string& message) {

  string::size_type length = message.length();

  if (bucketType == context_log) {
    // the key is in ascii after the third delimiter
    char delim = 1;
    string::size_type pos = 0;
    for (int i = 0; i < 3; ++i) {
      pos = message.find(delim, pos);
      if (pos == string::npos || length <= pos + 1) {
        return 0;
      }
      ++pos;
    }
    if (message[pos] == delim) {
      return 0;
    }

    uint32_t id = strtoul(message.substr(pos).c_str(), NULL, 10);
    if (id == 0) {
      return 0;
    }

    if (numBuckets == 0) {
      return 0;
    } else {
      return (integerhash::hash32(id) % numBuckets) + 1;
    }
  } else if (bucketType == random) {
    // return any random bucket
    return (rand() % numBuckets) + 1;
  } else {
    // just hash everything before the first user-defined delimiter
    string::size_type pos = message.find(delimiter);
    if (pos == string::npos) {
      // if no delimiter found, write to bucket 0
      return 0;
    }

    string key = message.substr(0, pos).c_str();
    if (key.empty()) {
      // if no key found, write to bucket 0
      return 0;
    }

    if (numBuckets == 0) {
      return 0;
    } else {
      switch (bucketType) {
        case key_modulo:
          // No hashing, just simple modulo
          return (atol(key.c_str()) % numBuckets) + 1;
          break;
        case key_range:
          if (bucketRange == 0) {
            return 0;
          } else {
            // Calculate what bucket this key would fall into if we used
            // bucket_range to compute the modulo
           double key_mod = atol(key.c_str()) % bucketRange;
           return (unsigned long) ((key_mod / bucketRange) * numBuckets) + 1;
          }
          break;
        case key_hash:
        default:
          // Hashing by default.
          return (strhash::hash32(key.c_str()) % numBuckets) + 1;
          break;
      }
    }
  }

  return 0;
}

string BucketStore::getMessageWithoutKey(const std::string& message) {
  string::size_type pos = message.find(delimiter);

  if (pos == string::npos) {
    return message;
  }

  return message.substr(pos+1);
}


NullStore::NullStore(const std::string& category, bool multi_category)
  : Store(category, "null", multi_category)
{}

NullStore::~NullStore() {
}

boost::shared_ptr<Store> NullStore::copy(const std::string &category) {
  NullStore *store = new NullStore(category, multiCategory);
  shared_ptr<Store> copied = shared_ptr<Store>(store);
  return copied;
}

bool NullStore::open() {
  return true;
}

bool NullStore::isOpen() {
  return true;
}

void NullStore::configure(pStoreConf) {
}

void NullStore::close() {
}

bool NullStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {
  incCounter(categoryHandled, "ignored", messages->size());
  return true;
}

void NullStore::flush() {
}

bool NullStore::readOldest(/*out*/ boost::shared_ptr<logentry_vector_t> messages,
                       struct tm* now) {
  return true;
}

bool NullStore::replaceOldest(boost::shared_ptr<logentry_vector_t> messages,
                              struct tm* now) {
  return true;
}

void NullStore::deleteOldest(struct tm* now) {
}

bool NullStore::empty(struct tm* now) {
  return true;
}

MultiStore::MultiStore(const std::string& category, bool multi_category)
  : Store(category, "multi", multi_category) {
}

MultiStore::~MultiStore() {
}

boost::shared_ptr<Store> MultiStore::copy(const std::string &category) {
  MultiStore *store = new MultiStore(category, multiCategory);
  store->report_success = this->report_success;
  boost::shared_ptr<Store> tmp_copy;
  for (std::vector<boost::shared_ptr<Store> >::iterator iter = stores.begin();
       iter != stores.end();
       ++iter) {
    tmp_copy = (*iter)->copy(category);
    store->stores.push_back(tmp_copy);
  }

  return shared_ptr<Store>(store);
}

bool MultiStore::open() {
  bool all_result = true;
  bool any_result = false;
  bool cur_result;
  for (std::vector<boost::shared_ptr<Store> >::iterator iter = stores.begin();
       iter != stores.end();
       ++iter) {
    cur_result = (*iter)->open();
    any_result |= cur_result;
    all_result &= cur_result;
  }
  return (report_success == SUCCESS_ALL) ? all_result : any_result;
}

bool MultiStore::isOpen() {
  bool all_result = true;
  bool any_result = false;
  bool cur_result;
  for (std::vector<boost::shared_ptr<Store> >::iterator iter = stores.begin();
       iter != stores.end();
       ++iter) {
    cur_result = (*iter)->isOpen();
    any_result |= cur_result;
    all_result &= cur_result;
  }
  return (report_success == SUCCESS_ALL) ? all_result : any_result;
}

void MultiStore::configure(pStoreConf configuration) {
  /**
   * in this store, we look for other numbered stores
   * in the following fashion:
   * <store>
   *   type=multi
   *   report_success=all|any
   *   <store0>
   *     ...
   *   </store0>
       ...
   *   <storen>
   *     ...
   *   </storen>
   * </store>
   */
  pStoreConf cur_conf;
  string cur_type;
  boost::shared_ptr<Store> cur_store;
  string report_preference;

  // find reporting preference
  if (configuration->getString("report_success", report_preference)) {
    if (0 == report_preference.compare("all")) {
      report_success = SUCCESS_ALL;
      LOG_OPER("[%s] MULTI: Logging success only if all stores succeed.",
               categoryHandled.c_str());
    } else if (0 == report_preference.compare("any")) {
      report_success = SUCCESS_ANY;
      LOG_OPER("[%s] MULTI: Logging success if any store succeeds.",
               categoryHandled.c_str());
    } else {
      LOG_OPER("[%s] MULTI: %s is an invalid value for report_success.",
               categoryHandled.c_str(), report_preference.c_str());
      setStatus("MULTI: Invalid report_success value.");
      return;
    }
  } else {
    report_success = SUCCESS_ALL;
  }

  // find stores
  for (int i=0; ;++i) {
    stringstream ss;
    ss << "store" << i;
    if (!configuration->getStore(ss.str(), cur_conf)) {
      // allow this to be 0 or 1 indexed
      if (i == 0) {
        continue;
      }

      // no store for this id? we're finished.
      break;
    } else {
      // find this store's type
      if (!cur_conf->getString("type", cur_type)) {
        LOG_OPER("[%s] MULTI: Store %d is missing type.", categoryHandled.c_str(), i);
        setStatus("MULTI: Store is missing type.");
        return;
      } else {
        // add it to the list
        cur_store = createStore(cur_type, categoryHandled, false, multiCategory);
        LOG_OPER("[%s] MULTI: Configured store of type %s successfully.",
                 categoryHandled.c_str(), cur_type.c_str());
        cur_store->configure(cur_conf);
        stores.push_back(cur_store);
      }
    }
  }

  if (stores.size() == 0) {
    setStatus("MULTI: No stores found, invalid store.");
    LOG_OPER("[%s] MULTI: No stores found, invalid store.", categoryHandled.c_str());
  }
}

void MultiStore::close() {
  for (std::vector<boost::shared_ptr<Store> >::iterator iter = stores.begin();
       iter != stores.end();
       ++iter) {
    (*iter)->close();
  }
}

bool MultiStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {
  bool all_result = true;
  bool any_result = false;
  bool cur_result;
  for (std::vector<boost::shared_ptr<Store> >::iterator iter = stores.begin();
       iter != stores.end();
       ++iter) {
    cur_result = (*iter)->handleMessages(messages);
    any_result |= cur_result;
    all_result &= cur_result;
  }

  // We cannot accurately report the number of messages not handled as messages
  // can be partially handled by a subset of stores.  So a multistore failure
  // will over-record the number of lost messages.
  return (report_success == SUCCESS_ALL) ? all_result : any_result;
}

void MultiStore::periodicCheck() {
  for (std::vector<boost::shared_ptr<Store> >::iterator iter = stores.begin();
       iter != stores.end();
       ++iter) {
    (*iter)->periodicCheck();
  }
}

void MultiStore::flush() {
  for (std::vector<boost::shared_ptr<Store> >::iterator iter = stores.begin();
       iter != stores.end();
       ++iter) {
    (*iter)->flush();
  }
}

CategoryStore::CategoryStore(const std::string& category, bool multiCategory)
  : Store(category, "category", multiCategory) {
}

CategoryStore::CategoryStore(const std::string& category,
                             const std::string& name, bool multiCategory)
  : Store(category, name, multiCategory) {
}

CategoryStore::~CategoryStore() {
}

boost::shared_ptr<Store> CategoryStore::copy(const std::string &category) {
  CategoryStore *store = new CategoryStore(category, multiCategory);

  store->modelStore = modelStore->copy(category);

  return shared_ptr<Store>(store);
}

bool CategoryStore::open() {
  bool result = true;

  for (map<string, shared_ptr<Store> >::iterator iter = stores.begin();
      iter != stores.end();
      ++iter) {
    result &= iter->second->open();
  }

  return result;
}

bool CategoryStore::isOpen() {

  for (map<string, shared_ptr<Store> >::iterator iter = stores.begin();
      iter != stores.end();
      ++iter) {
    if (!iter->second->isOpen()) {
      return false;
    }
  }

  return true;
}

void CategoryStore::configure(pStoreConf configuration) {
  /**
   *  Parse the store defined and use this store as a model to create a
   *  new store for every new category we see later.
   *  <store>
   *    type=category
   *    <model>
   *      type=...
   *      ...
   *    </model>
   *  </store>
   */
  pStoreConf cur_conf;

  if (!configuration->getStore("model", cur_conf)) {
    setStatus("CATEGORYSTORE: NO stores found, invalid store.");
    LOG_OPER("[%s] CATEGORYSTORE: No stores found, invalid store.",
             categoryHandled.c_str());
  } else {
    string cur_type;

    // find this store's type
    if (!cur_conf->getString("type", cur_type)) {
      LOG_OPER("[%s] CATEGORYSTORE: Store is missing type.",
               categoryHandled.c_str());
      setStatus("CATEGORYSTORE: Store is missing type.");
      return;
    }

    configureCommon(cur_conf, cur_type);
  }
}

void CategoryStore::configureCommon(pStoreConf configuration,
                                    const string type) {
  // initialize model store
  modelStore = createStore(type, categoryHandled, false, false);
  LOG_OPER("[%s] %s: Configured store of type %s successfully.",
           categoryHandled.c_str(), getType().c_str(), type.c_str());
  modelStore->configure(configuration);
}

void CategoryStore::close() {
  for (map<string, shared_ptr<Store> >::iterator iter = stores.begin();
      iter != stores.end();
      ++iter) {
    iter->second->close();
  }
}

bool CategoryStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {
  shared_ptr<logentry_vector_t> singleMessage(new logentry_vector_t);
  shared_ptr<logentry_vector_t> failed_messages(new logentry_vector_t);
  logentry_vector_t::iterator message_iter;

  for (message_iter = messages->begin();
      message_iter != messages->end();
      ++message_iter) {
    map<string, shared_ptr<Store> >::iterator store_iter;
    shared_ptr<Store> store;
    string category = (*message_iter)->category;

    store_iter = stores.find(category);

    if (store_iter == stores.end()) {
      // Create new store for this category
      store = modelStore->copy(category);
      store->open();
      stores[category] = store;
    } else {
      store = store_iter->second;
    }

    if (store == NULL || !store->isOpen()) {
      LOG_OPER("[%s] Failed to open store for category <%s>",
               categoryHandled.c_str(), category.c_str());
      failed_messages->push_back(*message_iter);
      continue;
    }

    // send this message to the store that handles this category
    singleMessage->clear();
    singleMessage->push_back(*message_iter);

    if (!store->handleMessages(singleMessage)) {
      LOG_OPER("[%s] Failed to handle message for category <%s>",
               categoryHandled.c_str(), category.c_str());
      failed_messages->push_back(*message_iter);
      continue;
    }
  }

  if (!failed_messages->empty()) {
    // Did not handle all messages, update message vector
    messages->swap(*failed_messages);
    return false;
  } else {
    return true;
  }
}

void CategoryStore::periodicCheck() {
  for (map<string, shared_ptr<Store> >::iterator iter = stores.begin();
      iter != stores.end();
      ++iter) {
    iter->second->periodicCheck();
  }
}

void CategoryStore::flush() {
  for (map<string, shared_ptr<Store> >::iterator iter = stores.begin();
      iter != stores.end();
      ++iter) {
    iter->second->flush();
  }
}

MultiFileStore::MultiFileStore(const std::string& category, bool multi_category)
  : CategoryStore(category, "MultiFileStore", multi_category) {
}

MultiFileStore::~MultiFileStore() {
}

void MultiFileStore::configure(pStoreConf configuration) {
  configureCommon(configuration, "file");
}

ThriftMultiFileStore::ThriftMultiFileStore(const std::string& category,
                                           bool multi_category)
  : CategoryStore(category, "ThriftMultiFileStore", multi_category) {
}

ThriftMultiFileStore::~ThriftMultiFileStore() {
}

void ThriftMultiFileStore::configure(pStoreConf configuration) {
  configureCommon(configuration, "thriftfile");
}
