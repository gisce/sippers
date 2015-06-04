from __future__ import absolute_import
import zipfile

import click
from sippers.file import SipsFile, PackedSipsFile
from sippers.backends import get_backend

@click.group()
def sippers():
    pass



@sippers.command(name="import")
@click.option('--file', help="SIPS File to import", type=click.Path(exists=True), required=True)
@click.option('--backend', help="Backend url", required=True)
def import_file(file, backend):
    url = backend
    backend = get_backend(backend)
    with backend(url) as bnd:
        if zipfile.is_zipfile(file):
            click.echo("Using packed SIPS File for {}".format(file))
            with PackedSipsFile(file) as psf:
                pstats = psf.stats
                for sips_file in psf:
                    print sips_file.path
                    stats = sips_file.stats
                    for line in sips_file:
                        if not line:
                            continue
                        bnd.insert(line)
                        print pstats.progress, stats.progress, stats.elapsed_time, stats.speed
        else:
            with SipsFile(file) as sips_file:
                stats = sips_file.stats
                for line in sips_file:
                    if not line:
                        continue
                    bnd.insert(line)
                    print stats.progress, stats.elapsed_time, stats.speed



if __name__ == "__main__":
    sippers()

