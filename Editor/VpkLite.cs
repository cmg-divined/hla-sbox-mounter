using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Sandbox
{
	/// <summary>
	/// Minimal VPK reader sufficient for HL:A asset mounting (v1/v2 compatible).
	/// Stores a tree of entries by extension->directory->files.
	/// </summary>
	public sealed class VpkPackage : IDisposable
	{
		public const int MAGIC = 0x55AA1234;
		private FileStream _fs;
		private BinaryReader _br;
		public string FileName { get; private set; }
		public uint Version { get; private set; }
		public uint HeaderSize { get; private set; }
		public uint TreeSize { get; private set; }
		public uint FileDataSectionSize { get; private set; }

		public readonly Dictionary<string, Dictionary<string, List<VpkFileEntry>>> Entries = new();

		public void Read( string fileName )
		{
			FileName = fileName;
			_fs = new FileStream( fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite );
			_br = new BinaryReader( _fs );
			ReadHeader();
			ReadEntries();
		}

		private void ReadHeader()
		{
			var sig = _br.ReadUInt32();
			if ( sig != MAGIC ) throw new InvalidDataException( "Not a VPK" );
			Version = _br.ReadUInt32();
			TreeSize = _br.ReadUInt32();
			if ( Version == 2 )
			{
				FileDataSectionSize = _br.ReadUInt32();
				_ = _br.ReadUInt32(); // archive md5
				_ = _br.ReadUInt32(); // other md5
				_ = _br.ReadUInt32(); // signature
			}
			HeaderSize = (uint)_fs.Position;
		}

		private void ReadEntries()
		{
			while ( true )
			{
				var ext = ReadZ();
				if ( string.IsNullOrEmpty( ext ) ) break;
				if ( !Entries.TryGetValue( ext, out var dirs ) ) Entries[ext] = dirs = new();
				while ( true )
				{
					var dir = ReadZ();
					if ( string.IsNullOrEmpty( dir ) ) break;
					if ( !dirs.TryGetValue( dir, out var list ) ) dirs[dir] = list = new();
					while ( true )
					{
						var name = ReadZ();
						if ( string.IsNullOrEmpty( name ) ) break;
						var crc = _br.ReadUInt32();
						var preload = _br.ReadUInt16();
						var archiveIndex = _br.ReadUInt16();
						var offset = _br.ReadUInt32();
						var length = _br.ReadUInt32();
						var term = _br.ReadUInt16();
						var e = new VpkFileEntry
						{
							FileName = name,
							DirectoryName = dir,
							TypeName = ext,
							CRC32 = crc,
							ArchiveIndex = archiveIndex,
							Offset = offset,
							Length = length,
							SmallData = preload > 0 ? _br.ReadBytes( preload ) : Array.Empty<byte>(),
							Package = this
						};
						list.Add( e );
					}
				}
			}
		}

		private string ReadZ()
		{
			var buf = new List<byte>( 64 );
			byte b;
			while ( (b = _br.ReadByte()) != 0 ) buf.Add( b );
			return Encoding.UTF8.GetString( buf.ToArray() );
		}

		public byte[] ReadEntry( VpkFileEntry entry )
		{
			var result = new byte[entry.TotalLength];
			var ofs = 0;
			if ( entry.SmallData.Length > 0 )
			{
				Buffer.BlockCopy( entry.SmallData, 0, result, 0, entry.SmallData.Length );
				ofs += entry.SmallData.Length;
			}
			if ( entry.Length > 0 )
			{
				if ( entry.ArchiveIndex == 0x7FFF )
				{
					_fs.Seek( HeaderSize + TreeSize + entry.Offset, SeekOrigin.Begin );
					_fs.Read( result, ofs, (int)entry.Length );
				}
				else
				{
					var chunk = FileName.Replace( "_dir.vpk", $"_{entry.ArchiveIndex:D3}.vpk" );
					using var afs = new FileStream( chunk, FileMode.Open, FileAccess.Read, FileShare.ReadWrite );
					afs.Seek( entry.Offset, SeekOrigin.Begin );
					afs.Read( result, ofs, (int)entry.Length );
				}
			}
			return result;
		}

		public void Dispose()
		{
			_br?.Dispose();
			_fs?.Dispose();
		}
	}

	public sealed class VpkFileEntry
	{
		public string FileName;
		public string DirectoryName;
		public string TypeName;
		public uint CRC32;
		public uint Length;
		public uint Offset;
		public ushort ArchiveIndex;
		public byte[] SmallData = Array.Empty<byte>();
		public VpkPackage Package;
		public uint TotalLength => Length + (uint)SmallData.Length;
		public string GetFullPath()
		{
			var dir = string.IsNullOrEmpty( DirectoryName ) || DirectoryName == " " ? string.Empty : DirectoryName + "/";
			var ext = string.IsNullOrEmpty( TypeName ) || TypeName == " " ? string.Empty : "." + TypeName;
			return (dir + FileName + ext).Replace( '\\', '/' ).ToLowerInvariant();
		}
	}
}




