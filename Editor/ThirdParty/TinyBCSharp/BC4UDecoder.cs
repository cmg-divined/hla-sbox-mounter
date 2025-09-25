using System.Buffers.Binary;

using System;

namespace TinyBCSharp;

class BC4UDecoder(bool grayscale)
    : BC4Decoder(grayscale)
{
    public override void DecodeBlock(ReadOnlySpan<byte> src, Span<byte> dst, int stride)
    {
        var block = BinaryPrimitives.ReadInt64LittleEndian(src);

        // @formatter:off
        var a0 = (int)  block       & 0xFF;
        var a1 = (int) (block >> 8) & 0xFF;

        var alphas = (stackalloc byte[8]);
        alphas[0] = (byte)a0;
        alphas[1] = (byte)a1;
        
        if (a0 > a1) {
            alphas[2] = Scale1785(6 * a0 +     a1);
            alphas[3] = Scale1785(5 * a0 + 2 * a1);
            alphas[4] = Scale1785(4 * a0 + 3 * a1);
            alphas[5] = Scale1785(3 * a0 + 4 * a1);
            alphas[6] = Scale1785(2 * a0 + 5 * a1);
            alphas[7] = Scale1785(    a0 + 6 * a1);
        } else {
            alphas[2] = Scale1275(4 * a0 +     a1);
            alphas[3] = Scale1275(3 * a0 + 2 * a1);
            alphas[4] = Scale1275(2 * a0 + 3 * a1);
            alphas[5] = Scale1275(    a0 + 4 * a1);
            alphas[7] = 0xFF;
        }
        // @formatter:on

        var indices = block >> 16;
        Write(dst, stride, alphas, indices);
    }

    static byte Scale1785(int i) => (byte)((i * 585 + 2010) >> 12);
    static byte Scale1275(int i) => (byte)((i * 819 + 1893) >> 12);
}