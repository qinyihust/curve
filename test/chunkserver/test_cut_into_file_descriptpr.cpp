#include <fcntl.h>
#include <butil/iobuf.h>

int main() {
    char header_buf[24];
    butil::IOBuf header;
    header.append(header_buf, 24);
    butil::IOBuf data;
    char data_buf[4119];
    data.append(data_buf, 4119);
    const size_t to_write = header.length() + data.length();
    butil::IOBuf* pieces[2] = { &header, &data };
    int fd = ::open("/data/chunkserver0/test_sync.file", O_RDWR|O_CREAT, 0644);
    
}