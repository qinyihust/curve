

#include <unistd.h>
#include <fcntl.h>
#include <sys/time.h>
#include <butil/iobuf.h>
#include <iostream>

inline uint64_t GetTimeofDayUs() {
    timeval now;
    gettimeofday(&now, NULL);
    return now.tv_sec * 1000000L + now.tv_usec;
}

int main() {
    int fd = ::open("test_sync.file", O_RDWR | O_CREAT | O_TRUNC, 0644);
    for (int i = 0; i < 100; ++i) {
        char* data = new char[4096];
        memset(data, 0, 4096);
        char* header_buf = new char[24];
        butil::IOBuf iobuf;
        iobuf.append(header_buf, 24);
        iobuf.append(data, 4096);
        iobuf.cut_into_file_descriptor(fd, 4120);
        int64_t start = GetTimeofDayUs();
        ::fsync(fd);
        int64_t end = GetTimeofDayUs();
        std::cout << end - start << std::endl;
        delete data;
        delete header_buf;
    }
    return 0;
}

