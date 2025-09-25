using System.Buffers.Binary;

using System;

namespace TinyBCSharp;

class BC1Decoder(BC1Mode mode)
    : BlockDecoder(8, BytesPerPixel)
{
    const int BytesPerPixel = 4;

    readonly bool _bc2Or3 = mode == BC1Mode.BC2Or3;
    readonly uint _color3 = mode == BC1Mode.Opaque ? 0xFF000000 : 0;

    public override void DecodeBlock(ReadOnlySpan<byte> src, Span<byte> dst, int stride)
    {
        var block = BinaryPrimitives.ReadUInt64LittleEndian(src);
            
        // @formatter:off
        var c0 = (uint) block        & 0xFFFF;
        var c1 = (uint)(block >> 16) & 0xFFFF;

        var r0 = (c0 >> 11) & 0x1F;
        var g0 = (c0 >>  5) & 0x3F;
        var b0 =  c0        & 0x1F;

        var r1 = (c1 >> 11) & 0x1F;
        var g1 = (c1 >>  5) & 0x3F;
        var b1 =  c1        & 0x1F;
        // @formatter:on

        var colors = (stackalloc uint[4]);
        colors[0] = RGB(Scale031(r0), Scale063(g0), Scale031(b0));
        colors[1] = RGB(Scale031(r1), Scale063(g1), Scale031(b1));

        if (c0 > c1 || _bc2Or3)
        {
            var r2 = Scale093(2 * r0 + r1);
            var g2 = Scale189(2 * g0 + g1);
            var b2 = Scale093(2 * b0 + b1);
            colors[2] = RGB(r2, g2, b2);

            var r3 = Scale093(r0 + 2 * r1);
            var g3 = Scale189(g0 + 2 * g1);
            var b3 = Scale093(b0 + 2 * b1);
            colors[3] = RGB(r3, g3, b3);
        }
        else
        {
            var r2 = Scale062(r0 + r1);
            var g2 = Scale126(g0 + g1);
            var b2 = Scale062(b0 + b1);
            colors[2] = RGB(r2, g2, b2);
            colors[3] = _color3;
        }

        var indices = (int)(block >> 32);
        for (var y = 0; y < BlockHeight; y++)
        {
            var dstPos = y * stride;
            for (var x = 0; x < BlockWidth; x++)
            {
                var index = dstPos + x * BytesPerPixel;
                var color = colors[indices & 3];
                BinaryPrimitives.WriteUInt32LittleEndian(dst[index..], color);
                indices >>= 2;
            }
        }
    }

    static uint RGB(uint r, uint g, uint b) => r | g << 8 | b << 16 | 0xFF000000;
    static uint Scale031(uint i) => (i * 527 + 23) >> 6;
    static uint Scale063(uint i) => (i * 259 + 33) >> 6;
    static uint Scale093(uint i) => (i * 351 + 61) >> 7;
    static uint Scale189(uint i) => (i * 2763 + 1039) >> 11;
    static uint Scale062(uint i) => (i * 1053 + 125) >> 8;
    static uint Scale126(uint i) => (i * 4145 + 1019) >> 11;
}