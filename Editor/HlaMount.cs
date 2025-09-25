using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Sandbox.Mounting;
using SysIO = System.IO;

namespace Sandbox
{
	/// <summary>
	/// Half-Life: Alyx mount. Scans HL:A VPKs and exposes Source 2 resources to s&box.
	/// Keep all code self-contained in the hla project (no external refs).
	/// </summary>
	public sealed class HlaMount : BaseGameMount
	{
		private const long AppId = 546560L; // Half-Life: Alyx

		private string _appDir;

		// Minimal Source2 compiled extension map -> s&box ResourceType
		private static readonly Dictionary<string, ResourceType> FileTypes = new()
		{
			{ ".vtex_c", ResourceType.Texture },
			{ ".vmat_c", ResourceType.Material },
			{ ".vmdl_c", ResourceType.Model },
			{ ".vmesh_c", ResourceType.Model },
			{ ".vanim_c", ResourceType.Model },
			{ ".vseq_c", ResourceType.Model },
			{ ".vmap_c", ResourceType.Scene },
			{ ".vwrld_c", ResourceType.Scene },
			{ ".vwnod_c", ResourceType.Scene },
			{ ".vsnd_c", ResourceType.Sound }
		};

		// VPK container support (local, no external library)
		private readonly List<VpkPackage> _packages = new();
		private readonly Dictionary<string, VpkFileEntry> _index = new();

		public override string Ident => "hla";
		public override string Title => "Half-Life: Alyx";

		protected override void Initialize( InitializeContext context )
		{
			if ( context.IsAppInstalled( AppId ) )
			{
				_appDir = context.GetAppDirectory( AppId );
				IsInstalled = Path.Exists( _appDir );
			}
		}

		protected override Task Mount( MountContext context )
		{
			if ( string.IsNullOrEmpty( _appDir ) || !SysIO.Directory.Exists( _appDir ) )
			{
				context.AddError( "HL:A directory not found" );
				return Task.CompletedTask;
			}

			LoadVpkPackages();
			IndexVpkFiles();
			RegisterAssets( context );

			IsMounted = true;
			return Task.CompletedTask;
		}

		private void LoadVpkPackages()
		{
			_packages.Clear();

			// Common HL:A VPK roots
			var roots = new[]
			{
				SysIO.Path.Combine( _appDir, "game", "hlvr" ),
				SysIO.Path.Combine( _appDir, "game", "core" ),
				SysIO.Path.Combine( _appDir, "game" )
			};

			foreach ( var root in roots )
			{
				if ( !SysIO.Directory.Exists( root ) ) continue;

				foreach ( var vpk in SysIO.Directory.GetFiles( root, "*_dir.vpk", SearchOption.AllDirectories ) )
				{
					try
					{
						var pkg = new VpkPackage();
						pkg.Read( vpk );
						_packages.Add( pkg );
					}
					catch ( Exception e )
					{
						Log.Warning( $"Failed to read VPK '{vpk}': {e.Message}" );
					}
				}
			}
		}

		private void IndexVpkFiles()
		{
			_index.Clear();
			foreach ( var pkg in _packages )
			{
				foreach ( var (ext, dirs) in pkg.Entries )
				{
					foreach ( var (dir, files) in dirs )
					{
						foreach ( var f in files )
						{
							f.Package = pkg;
							_index[f.GetFullPath()] = f;
						}
					}
				}
			}
		}

		private void RegisterAssets( MountContext context )
		{
			foreach ( var (path, entry) in _index )
			{
				var ext = SysIO.Path.GetExtension( path ).ToLowerInvariant();
				if ( !FileTypes.TryGetValue( ext, out var type ) ) continue;

				// Strip trailing `_c` for s&box logical path
				var sboxPath = path.EndsWith( "_c", StringComparison.Ordinal ) ? path[..^2] : path;

				switch ( type )
				{
					case ResourceType.Texture:
						context.Add( type, sboxPath, new HLATextureLoader( this, path ) );
						break;
					case ResourceType.Material:
						context.Add( type, sboxPath, new HLAMaterialLoader( this, path ) );
						break;
					case ResourceType.Model:
						context.Add( type, sboxPath, new HLAModelLoader( this, path ) );
						break;
					default:
						// Omit scenes/sounds for now or add placeholders later
						break;
				}
			}
		}

		public byte[] GetFileBytes( string filePath )
		{
			filePath = filePath.Replace( '\\', '/' ).ToLowerInvariant();
			if ( _index.TryGetValue( filePath, out var e ) )
			{
				return e.Package.ReadEntry( e );
			}
			throw new FileNotFoundException( $"Not found in HL:A VPKs: {filePath}" );
		}

		public Stream GetFileStream( string filePath )
		{
			return new MemoryStream( GetFileBytes( filePath ) );
		}
	}
}




