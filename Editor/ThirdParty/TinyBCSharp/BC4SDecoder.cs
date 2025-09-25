using System.Buffers.Binary;

using System;

namespace TinyBCSharp;

class BC4SDecoder(bool grayscale)
    : BC4Decoder(grayscale)
{
    public override void DecodeBlock(ReadOnlySpan<byte> src, Span<byte> dst, int stride)
    {
        var block = BinaryPrimitives.ReadInt64LittleEndian(src);

        // @formatter:off
        var a0 = (int)(sbyte)  block;
        var a1 = (int)(sbyte) (block >> 8);

        var alphas = (stackalloc byte[8]);
        alphas[0] = Scale127(a0);
        alphas[1] = Scale127(a1);
        
        if (a0 > a1) {
            alphas[2] = Scale889(6 * a0 +     a1);
            alphas[3] = Scale889(5 * a0 + 2 * a1);
            alphas[4] = Scale889(4 * a0 + 3 * a1);
            alphas[5] = Scale889(3 * a0 + 4 * a1);
            alphas[6] = Scale889(2 * a0 + 5 * a1);
            alphas[7] = Scale889(    a0 + 6 * a1);
        } else {
            alphas[2] = Scale635(4 * a0 +     a1);
            alphas[3] = Scale635(3 * a0 + 2 * a1);
            alphas[4] = Scale635(2 * a0 + 3 * a1);
            alphas[5] = Scale635(    a0 + 4 * a1);
            alphas[7] = 0xFF;
        }
        // @formatter:on

        var indices = block >> 16;
        Write(dst, stride, alphas, indices);
    }

    static byte Scale127(int i) => (byte)((i * 129 + 16384) >> 7);
    static byte Scale889(int i) => (byte)((i * 75193 + 67108864) >> 19);
    static byte Scale635(int i) => (byte)((i * 13159 + 8388708) >> 16);
}