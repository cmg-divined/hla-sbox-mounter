using System.Buffers.Binary;

using System;

namespace TinyBCSharp;

class BC6HDecoder(bool signed, bool asSingle)
    : BPTCDecoder(16, asSingle ? 16 : 8)
{
    // @formatter:off
    static readonly Mode[] Modes =
    [
        new(T, T, 10, +5, +5, +5, [ 0x0941, 0x0A41, 0x0E41, 0x000A, 0x010A, 0x020A, 0x0405, 0x0D41, 0x0904, 0x0505, 0x0E01, 0x0D04, 0x0605, 0x0E11, 0x0A04, 0x0805, 0x0E21, 0x0C05, 0x0E31 ]),
        new(T, T, +7, +6, +6, +6, [ 0x0951, 0x0D41, 0x0D51, 0x0007, 0x0E01, 0x0E11, 0x0A41, 0x0107, 0x0A51, 0x0E21, 0x0941, 0x0207, 0x0E31, 0x0E51, 0x0E41, 0x0406, 0x0904, 0x0506, 0x0D04, 0x0606, 0x0A04, 0x0806, 0x0C06 ]),
        new(T, T, 11, +5, +4, +4, [ 0x000A, 0x010A, 0x020A, 0x0405, 0x00A1, 0x0904, 0x0504, 0x01A1, 0x0E01, 0x0D04, 0x0604, 0x02A1, 0x0E11, 0x0A04, 0x0805, 0x0E21, 0x0C05, 0x0E31 ]),
        new(T, T, 11, +4, +5, +4, [ 0x000A, 0x010A, 0x020A, 0x0404, 0x00A1, 0x0D41, 0x0904, 0x0505, 0x01A1, 0x0D04, 0x0604, 0x02A1, 0x0E11, 0x0A04, 0x0804, 0x0E01, 0x0E21, 0x0C04, 0x0941, 0x0E31 ]),
        new(T, T, 11, +4, +4, +5, [ 0x000A, 0x010A, 0x020A, 0x0404, 0x00A1, 0x0A41, 0x0904, 0x0504, 0x01A1, 0x0E01, 0x0D04, 0x0605, 0x02A1, 0x0A04, 0x0804, 0x0E11, 0x0E21, 0x0C04, 0x0E41, 0x0E31 ]),
        new(T, T, +9, +5, +5, +5, [ 0x0009, 0x0A41, 0x0109, 0x0941, 0x0209, 0x0E41, 0x0405, 0x0D41, 0x0904, 0x0505, 0x0E01, 0x0D04, 0x0605, 0x0E11, 0x0A04, 0x0805, 0x0E21, 0x0C05, 0x0E31 ]),
        new(T, T, +8, +6, +5, +5, [ 0x0008, 0x0D41, 0x0A41, 0x0108, 0x0E21, 0x0941, 0x0208, 0x0E31, 0x0E41, 0x0406, 0x0904, 0x0505, 0x0E01, 0x0D04, 0x0605, 0x0E11, 0x0A04, 0x0806, 0x0C06 ]),
        new(T, T, +8, +5, +6, +5, [ 0x0008, 0x0E01, 0x0A41, 0x0108, 0x0951, 0x0941, 0x0208, 0x0D51, 0x0E41, 0x0405, 0x0D41, 0x0904, 0x0506, 0x0D04, 0x0605, 0x0E11, 0x0A04, 0x0805, 0x0E21, 0x0C05, 0x0E31 ]),
        new(T, T, +8, +5, +5, +6, [ 0x0008, 0x0E11, 0x0A41, 0x0108, 0x0A51, 0x0941, 0x0208, 0x0E51, 0x0E41, 0x0405, 0x0D41, 0x0904, 0x0505, 0x0E01, 0x0D04, 0x0606, 0x0A04, 0x0805, 0x0E21, 0x0C05, 0x0E31 ]),
        new(F, T, +6, +6, +6, +6, [ 0x0006, 0x0D41, 0x0E01, 0x0E11, 0x0A41, 0x0106, 0x0951, 0x0A51, 0x0E21, 0x0941, 0x0206, 0x0D51, 0x0E31, 0x0E51, 0x0E41, 0x0406, 0x0904, 0x0506, 0x0D04, 0x0606, 0x0A04, 0x0806, 0x0C06 ]),
        new(F, F, 10, 10, 10, 10, [ 0x000A, 0x010A, 0x020A, 0x040A, 0x050A, 0x060A ]),
        new(T, F, 11, +9, +9, +9, [ 0x000A, 0x010A, 0x020A, 0x0409, 0x00A1, 0x0509, 0x01A1, 0x0609, 0x02A1 ]),
        new(T, F, 12, +8, +8, +8, [ 0x000A, 0x010A, 0x020A, 0x0408, 0x10A2, 0x0508, 0x11A2, 0x0608, 0x12A2 ]),
        new(T, F, 16, +4, +4, +4, [ 0x000A, 0x010A, 0x020A, 0x0404, 0x10A6, 0x0504, 0x11A6, 0x0604, 0x12A6 ])
    ];
    // @formatter:on

    const short OneHalf = 0x3C00;
    const int OneSingle = 0x3F800000;

    static readonly byte[] Reverse2 =
    [
        0, 2, 1, 3
    ];

    static readonly byte[] Reverse6 =
    [
        0, 32, 16, 48, +8, 40, 24, 56, 4, 36, 20, 52, 12, 44, 28, 60,
        2, 34, 18, 50, 10, 42, 26, 58, 6, 38, 22, 54, 14, 46, 30, 62,
        1, 33, 17, 49, +9, 41, 25, 57, 5, 37, 21, 53, 13, 45, 29, 61,
        3, 35, 19, 51, 11, 43, 27, 59, 7, 39, 23, 55, 15, 47, 31, 63
    ];

    public override void DecodeBlock(ReadOnlySpan<byte> src, Span<byte> dst, int stride)
    {
        var bits = Bits.From(src);
        var modeIndex = ModeIndex(ref bits);
        if (modeIndex >= Modes.Length)
        {
            FillInvalidBlock(dst, stride);
            return;
        }

        var mode = Modes[modeIndex];
        var colors = (stackalloc int[16]);
        foreach (var op in mode.Ops)
        {
            ReadOp(ref bits, op, colors);
        }

        var partition = mode.Pb ? bits.Get(5) : 0;
        var numPartitions = mode.Pb ? 2 : 1;

        if (signed)
        {
            colors[0] = ExtendSign(colors[0], mode.Epb);
            colors[1] = ExtendSign(colors[1], mode.Epb);
            colors[2] = ExtendSign(colors[2], mode.Epb);
        }

        if (mode.Te || signed)
        {
            for (var i = 4; i < numPartitions * 8; i += 4)
            {
                colors[i + 0] = ExtendSign(colors[i + 0], mode.Rb);
                colors[i + 1] = ExtendSign(colors[i + 1], mode.Gb);
                colors[i + 2] = ExtendSign(colors[i + 2], mode.Bb);
            }
        }

        if (mode.Te)
        {
            for (var i = 4; i < numPartitions * 8; i += 4)
            {
                colors[i + 0] = TransformInverse(colors[i + 0], colors[0], mode.Epb, signed);
                colors[i + 1] = TransformInverse(colors[i + 1], colors[1], mode.Epb, signed);
                colors[i + 2] = TransformInverse(colors[i + 2], colors[2], mode.Epb, signed);
            }
        }

        for (var i = 0; i < numPartitions * 8; i += 4)
        {
            colors[i + 0] = Unquantize(colors[i + 0], mode.Epb, signed);
            colors[i + 1] = Unquantize(colors[i + 1], mode.Epb, signed);
            colors[i + 2] = Unquantize(colors[i + 2], mode.Epb, signed);
        }

        var ib = numPartitions == 1 ? 4 : 3;
        var partitions = Partition(numPartitions, partition);
        var indexBits = IndexBits(ref bits, ib, numPartitions, partition);
        var weights = Weights(ib);
        var mask = (1 << ib) - 1;
        for (var y = 0; y < BlockHeight; y++)
        {
            var dstPos = y * stride;
            for (var x = 0; x < BlockWidth; x++)
            {
                int weight = weights[(int)(indexBits & mask)];
                indexBits >>= ib;

                var pIndex = (int)(partitions & 3);
                var r = FinalUnquantize(Interpolate(colors[pIndex * 8 + 0], colors[pIndex * 8 + 4], weight), signed);
                var g = FinalUnquantize(Interpolate(colors[pIndex * 8 + 1], colors[pIndex * 8 + 5], weight), signed);
                var b = FinalUnquantize(Interpolate(colors[pIndex * 8 + 2], colors[pIndex * 8 + 6], weight), signed);
                partitions >>= 2;

                if (asSingle)
                {
                    WritePixelF32(dst[(dstPos + x * 16)..], r, g, b);
                }
                else
                {
                    WritePixelF16(dst[(dstPos + x * 8)..], r, g, b);
                }
            }
        }
    }

    void FillInvalidBlock(Span<byte> dst, int stride)
    {
        if (asSingle)
        {
            for (var y = 0; y < BlockHeight; y++)
            {
                var dstPos = y * stride;
                dst.Slice(dstPos, 4 * 16).Clear();
                for (var x = 0; x < BlockWidth; x++)
                {
                    var index = dstPos + x * 16 + 12;
                    BinaryPrimitives.WriteInt32LittleEndian(dst[index..], OneSingle);
                }
            }
        }
        else
        {
            for (var y = 0; y < BlockHeight; y++)
            {
                var dstPos = y * stride;
                dst.Slice(dstPos, 4 * 8).Clear();
                for (var x = 0; x < BlockWidth; x++)
                {
                    var index = dstPos + x * 8 + 6;
                    BinaryPrimitives.WriteInt16LittleEndian(dst[index..], OneHalf);
                }
            }
        }
    }

    static int ModeIndex(ref Bits bits)
    {
        var mode = bits.Get(2);
        return mode < 2 ? mode : bits.Get(3) + (mode * 8 - 14);
    }

    static void ReadOp(ref Bits bits, short op, Span<int> colors)
    {
        // count | shift << 4 | index << 8 | (reverse ? 1 : 0) << 12
        var count = op & 0x0F;
        var shift = (op >> 4) & 0x0F;
        var index = (op >> 8) & 0x0F;
        var reverse = (op >> 12) != 0;

        var value = bits.Get(count);
        if (reverse)
        {
            value = count switch
            {
                2 => Reverse2[value],
                6 => Reverse6[value],
                _ => throw new NotImplementedException()
            };
        }

        colors[index] |= value << shift;
    }

    static int ExtendSign(int value, int bits)
    {
        var signBit = 1 << (bits - 1);
        return (value ^ signBit) - signBit;
    }

    static int TransformInverse(int value, int value0, int bits, bool signed)
    {
        value = (value + value0) & ((1 << bits) - 1);
        return signed ? ExtendSign(value, bits) : value;
    }

    static int Unquantize(int value, int bits, bool signed)
    {
        return signed
            ? UnquantizeSigned(value, bits)
            : UnquantizeUnsigned(value, bits);
    }

    static int UnquantizeSigned(int value, int bits)
    {
        if (bits >= 16 || value == 0)
        {
            return value;
        }

        var sign = value < 0;
        value = Math.Abs(value);

        int unq;
        if (value >= (1 << (bits - 1)) - 1)
        {
            unq = 0x7FFF;
        }
        else
        {
            unq = ((value << 15) + 0x4000) >> (bits - 1);
        }

        return sign ? -unq : unq;
    }

    static int UnquantizeUnsigned(int value, int bits)
    {
        if (bits >= 15 || value == 0)
        {
            return value;
        }

        if (value == ((1 << bits) - 1))
        {
            return 0xFFFF;
        }

        return ((value << 15) + 0x4000) >> (bits - 1);
    }

    static short FinalUnquantize(int i, bool signed)
    {
        if (signed)
        {
            i = (short)i;
            return (short)(i < 0 ? (((-i) * 31) >> 5) + 0x8000 : (i * 31) >> 5);
        }
        else
        {
            return (short)((i * 31) >> 6);
        }
    }

    static void WritePixelF16(Span<byte> dst, short r, short g, short b)
    {
        BinaryPrimitives.WriteInt16LittleEndian(dst, r);
        BinaryPrimitives.WriteInt16LittleEndian(dst[2..], g);
        BinaryPrimitives.WriteInt16LittleEndian(dst[4..], b);
        BinaryPrimitives.WriteHalfLittleEndian(dst[6..], (Half)1.0f);
    }

    static void WritePixelF32(Span<byte> dst, short r, short g, short b)
    {
        BinaryPrimitives.WriteSingleLittleEndian(dst, (float)BitConverter.Int16BitsToHalf(r));
        BinaryPrimitives.WriteSingleLittleEndian(dst[4..], (float)BitConverter.Int16BitsToHalf(g));
        BinaryPrimitives.WriteSingleLittleEndian(dst[8..], (float)BitConverter.Int16BitsToHalf(b));
        BinaryPrimitives.WriteSingleLittleEndian(dst[12..], 1.0f);
    }

    record struct Mode(
        bool Te,
        bool Pb,
        byte Epb,
        byte Rb,
        byte Gb,
        byte Bb,
        short[] Ops
    );
}