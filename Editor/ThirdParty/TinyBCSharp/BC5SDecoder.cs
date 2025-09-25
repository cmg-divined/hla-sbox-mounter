using System;

namespace TinyBCSharp;

class BC5SDecoder(bool reconstructZ)
    : BC5Decoder(new BC4SDecoder(false), reconstructZ);