package main

func packInt32(b []byte, i int) {
    b[0] = byte(i)
    b[1] = byte(i >> 8)
    b[2] = byte(i >> 16)
    b[3] = byte(i >> 24)
}

func unpackInt32(b []byte) int {
    return int((int32(b[0])) |
               (int32(b[1]) << 8) |
               (int32(b[2]) << 16) |
               (int32(b[3]) << 24))
}
