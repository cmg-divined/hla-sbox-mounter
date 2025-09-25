using System;

namespace TinyBCSharp;

class BC5UDecoder(bool reconstructZ)
    : BC5Decoder(new BC4UDecoder(false), reconstructZ);