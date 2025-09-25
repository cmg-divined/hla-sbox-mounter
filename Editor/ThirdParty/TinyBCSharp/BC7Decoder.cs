using System.Buffers.Binary;

using System;

namespace TinyBCSharp;

class BC7Decoder()
    : BPTCDecoder(16, BytesPerPixel)
{
    const int BytesPerPixel = 4;

    static readonly Mode[] Modes =
    [
        new(3, 4, F, F, 4, 0, T, F, 3, 0),
        new(2, 6, F, F, 6, 0, F, T, 3, 0),
        new(3, 6, F, F, 5, 0, F, F, 2, 0),
        new(2, 6, F, F, 7, 0, T, F, 2, 0),
        new(1, 0, T, T, 5, 6, F, F, 2, 3),
        new(1, 0, T, F, 7, 8, F, F, 2, 2),
        new(1, 0, F, F, 7, 7, T, F, 4, 0),
        new(2, 6, F, F, 5, 5, T, F, 2, 0)
    ];

    public override void DecodeBlock(ReadOnlySpan<byte> src, Span<byte> dst, int stride)
    {
        var modeIndex = int.TrailingZeroCount(src[0]);
        if (modeIndex >= Modes.Length)
        {
            FillInvalidBlock(dst, stride);
            return;
        }

        var mode = Modes[modeIndex];

        var bits = Bits.From(src);
        bits.Get(modeIndex + 1);

        var partition = mode.Pb != 0 ? bits.Get(mode.Pb) : 0;
        var rotation = mode.Rb ? bits.Get(2) : 0;
        var selection = mode.Isb && bits.Get(1) != 0;

        // Great, switching from an int[][] to an int[], increased perf by 40%.
        // I'll take the small readability hit.
        var numColors = mode.Ns * 2;
        var colors = (stackalloc int[numColors * 4]);

        // Read colors
        for (var c = 0; c < 3; c++)
        {
            for (var i = 0; i < numColors; i++)
            {
                colors[i * 4 + c] = bits.Get(mode.Cb);
            }
        }

        // Read alphas
        if (mode.Ab != 0)
        {
            for (var i = 0; i < numColors; i++)
            {
                colors[i * 4 + 3] = bits.Get(mode.Ab);
            }
        }

        // Read endpoint p-bits
        if (mode.Epb)
        {
            for (var i = 0; i < numColors; i++)
            {
                var pBit = bits.Get1();
                for (var c = 0; c < 4; c++)
                {
                    colors[i * 4 + c] = (colors[i * 4 + c] << 1) | pBit;
                }
            }
        }

        // Read shared p-bits
        if (mode.Spb)
        {
            var sBit1 = bits.Get1();
            var sBit2 = bits.Get1();
            for (var c = 0; c < 4; c++)
            {
                colors[0 * 4 + c] = (colors[0 * 4 + c] << 1) | sBit1;
                colors[1 * 4 + c] = (colors[1 * 4 + c] << 1) | sBit1;
                colors[2 * 4 + c] = (colors[2 * 4 + c] << 1) | sBit2;
                colors[3 * 4 + c] = (colors[3 * 4 + c] << 1) | sBit2;
            }
        }

        // Unpack colors
        var extraBits = (mode.Epb ? 1 : 0) + (mode.Spb ? 1 : 0);
        var colorBits = mode.Cb + extraBits;
        var alphaBits = mode.Ab + extraBits;
        for (var i = 0; i < numColors; i++)
        {
            if (colorBits < 8)
            {
                colors[i * 4 + 0] = Unpack(colors[i * 4 + 0], colorBits);
                colors[i * 4 + 1] = Unpack(colors[i * 4 + 1], colorBits);
                colors[i * 4 + 2] = Unpack(colors[i * 4 + 2], colorBits);
            }

            if (mode.Ab != 0 && alphaBits < 8)
            {
                colors[i * 4 + 3] = Unpack(colors[i * 4 + 3], alphaBits);
            }
        }

        // Opaque mode
        if (mode.Ab == 0)
        {
            for (var i = 0; i < numColors; i++)
            {
                colors[i * 4 + 3] = 0xFF;
            }
        }

        // Let's try a new method
        var partitions = Partition(mode.Ns, partition);
        var indexBits1 = IndexBits(ref bits, mode.Ib1, mode.Ns, partition);
        var indexBits2 = IndexBits(ref bits, mode.Ib2, mode.Ns, partition);
        var weights1 = Weights(mode.Ib1);
        var weights2 = Weights(mode.Ib2);
        var mask1 = (1 << mode.Ib1) - 1;
        var mask2 = (1 << mode.Ib2) - 1;

        for (var y = 0; y < BlockHeight; y++)
        {
            var dstPos = y * stride;
            for (var x = 0; x < BlockWidth; x++)
            {
                int weight1 = weights1[(int)(indexBits1 & mask1)];
                var cWeight = weight1;
                var aWeight = weight1;
                indexBits1 >>= mode.Ib1;

                if (mode.Ib2 != 0)
                {
                    int weight2 = weights2[(int)(indexBits2 & mask2)];
                    if (selection)
                        cWeight = weight2;
                    else
                        aWeight = weight2;

                    indexBits2 >>= mode.Ib2;
                }

                var pIndex = (int)(partitions & 3);
                var r = Interpolate(colors[pIndex * 8 + 0], colors[pIndex * 8 + 4], cWeight);
                var g = Interpolate(colors[pIndex * 8 + 1], colors[pIndex * 8 + 5], cWeight);
                var b = Interpolate(colors[pIndex * 8 + 2], colors[pIndex * 8 + 6], cWeight);
                var a = Interpolate(colors[pIndex * 8 + 3], colors[pIndex * 8 + 7], aWeight);
                partitions >>= 2;

                if (rotation != 0)
                {
                    switch (rotation)
                    {
                        case 1:
                            (a, r) = (r, a);
                            break;
                        case 2:
                            (a, g) = (g, a);
                            break;
                        case 3:
                            (a, b) = (b, a);
                            break;
                    }
                }

                var index = dstPos + x * BytesPerPixel;
                var color = r | g << 8 | b << 16 | a << 24;
                BinaryPrimitives.WriteInt32LittleEndian(dst[index..], color);
            }
        }
    }

    static void FillInvalidBlock(Span<byte> dst, int stride)
    {
        for (var y = 0; y < BlockHeight; y++)
        {
            dst.Slice(y * stride, BlockWidth * BytesPerPixel).Clear();
        }
    }

    static int Unpack(int i, int n)
    {
        return i << (8 - n) | i >> (2 * n - 8);
    }

    record struct Mode(
        byte Ns,
        byte Pb,
        bool Rb,
        bool Isb,
        byte Cb,
        byte Ab,
        bool Epb,
        bool Spb,
        byte Ib1,
        byte Ib2
    );
}