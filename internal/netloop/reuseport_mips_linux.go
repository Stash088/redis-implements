//go:build linux && (mips || mipsle || mips64 || mips64le)

package netloop

// soReusePort is SO_REUSEPORT from the Linux kernel UAPI headers. The MIPS
// architectures use a different value than the rest of Linux.
const soReusePort = 0x200
