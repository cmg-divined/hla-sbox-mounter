using System.Buffers.Binary;

using System;

namespace TinyBCSharp;

struct Bits
{
    ulong _lo;
    ulong _hi;

    Bits(ulong lo, ulong hi)
    {
        _lo = lo;
        _hi = hi;
    }

    internal static Bits From(ReadOnlySpan<byte> array)
    {
        var lo = BinaryPrimitives.ReadUInt64LittleEndian(array);
        var hi = BinaryPrimitives.ReadUInt64LittleEndian(array[8..]);
        return new Bits(lo, hi);
    }

    internal long Get64(int count)
    {
        var bits = _lo & ((1UL << count) - 1);
        _lo = (_lo >> count) | (_hi << (64 - count));
        _hi = (_hi >> count);
        return (long)bits;
    }

    internal int Get(int count)
    {
        return (int)Get64(count);
    }

    internal int Get1()
    {
        return Get(1);
    }
}