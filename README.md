# 轻量级文件操作模块（本地二进制文件存储）

功能：实现对文件夹/文件的创建、删除、按偏移读写、按页读写（二进制）。

构建：使用 CMake

示例：

```bash
mkdir build && cd build
cmake ..
cmake --build . --config Release
./lightdb_demo
```

文件：
- include/FileManager.h
- src/FileManager.cpp
- src/main.cpp (示例)
