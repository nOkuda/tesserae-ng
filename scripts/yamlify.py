"""Do dirty ingest job for v4"""
import argparse
import os


def _parse_args():
    """Parses command line arguments"""
    parser = argparse.ArgumentParser(description='Quick ingest yaml maker')
    parser.add_argument(
        'dirname',
        help='path of directory containing texts for ingest')
    return parser.parse_args()


def _casify(section):
    """Computes proper casing"""
    return ' '.join([a.capitalize() for a in section.split('_')])


def _parse_filename(filename):
    """Computes author and title from filename"""
    sections = filename.split('.')
    return _casify(sections[0]), _casify(sections[1])


def _make_yaml(textroot, filename):
    """Makes yaml file"""
    author, title = _parse_filename(filename)
    with open(os.path.join(textroot, filename[:-4] + 'yaml'), 'w') as ofh:
        ofh.write('auto_ingest: true\n')
        ofh.write('index: true\n')
        ofh.write('language: latin\n')
        ofh.write('author: ' + author + '\n')
        ofh.write('title: ' + title + '\n')
        ofh.write('volumes:\n')
        ofh.write('  - name: Full Text\n')
        ofh.write('    path: ' + filename + '\n')
        dirname = filename[:-5]
        if os.path.isdir(dirname):
            partnames = []
            others = []
            for partname in os.listdir(dirname):
                preint = partname.split('.')[-2]
                try:
                    partnum = int(preint)
                    partnames.append((int(partnum), partname))
                except ValueError:
                    others.append((preint, partname))
            total = str(len(partnames))
            for (partnum, partname) in sorted(partnames):
                ofh.write(
                    '  - name: Book ' + str(partnum) + ' / ' + total + '\n')
                ofh.write(
                    '    path: ' + os.path.join(dirname, partname) + '\n')
            for (other, partname) in sorted(others):
                ofh.write(
                    '  - name: Book ' + _casify(str(other)) + '\n')
                ofh.write(
                    '    path: ' + os.path.join(dirname, partname) + '\n')


def _run():
    """Makes yaml files for texts to be ingested"""
    args = _parse_args()
    for filename in os.listdir(args.dirname):
        if os.path.isfile(filename) and filename.endswith('.tess'):
            _make_yaml(args.dirname, filename)


if __name__ == '__main__':
    _run()
