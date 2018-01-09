from __future__ import print_function, unicode_literals

import os
import re
import posixpath


from py7zlib import Archive7z
from zipfile import ZipFile
try:
    from urllib.request import urlopen
    from urllib.parse import urljoin
except ImportError:
    from urlparse import urljoin
    from urllib2 import urlopen

_ext = re.compile('\.([^.]+)$')
protocol = re.compile('^(\w+)://')


def get_ext(fname):
    return _ext.search(fname).groups()[0].lower()


class Archive(object):
    """
    A strictly local archive of files to be used as a repository
    """

    @staticmethod
    def _create_cache_dir(path):
        d = os.path.join(os.path.dirname(__file__), 'web_cache', protocol.sub('', path))
        if not os.path.isdir(d):
            os.makedirs(d)
        return d

    @staticmethod
    def _access_7z(path):
        try:
            fp = open(path, 'rb')
            archive = Archive7z(fp)
        except:
            archive = False
        return archive

    @staticmethod
    def _access_zip(path):
        try:
            archive = ZipFile(path)
        except:
            archive = False
        return archive

    def __init__(self, path, internal_prefix=None, query_string=None, cache=True):
        """
        Create an Archive object from a path.  Basically encapsulates the compression algorithm and presents
        a common interface to client code:
        self.listfiles()
        self.countfiles()
        self.readfile(filename)

        writing providers is not presently supported.

        By default remote archives create a local cache to store downloaded files- this cache is mounted as an
        ordinary archive and is checked first when readfile() is called.  You can force a re-download by specifying
        .readfile(..., force_download=True)

        :param path:
        :param internal_prefix: if present, silently absorb / conceal the specified prefix (prepend to requests, trim
         from responses).
        :param query_string: for remote repositories, append the supplied string after '?' in the URL
        :param cache: (True) for remote repositories, cache downloaded files locally and use first
        :return: an archive object
        """

        self.path = path
        self._internal_prefix = internal_prefix

        if bool(protocol.search(path)):
            self.ext = protocol.search(path).groups()[0]
            self.remote = True
            self.compressed = False
            self._archive = None
            self.OK = True
            self._internal_subfolders = []
            print('Archive refers to a web address using protocol %s' % self.ext)
            self.query_string = query_string
            if self.query_string is not None:
                print(' with query string %s' % self.query_string)
            self.cache = cache
            if self.cache:
                self._cache = Archive(self._create_cache_dir(self.path), internal_prefix=internal_prefix)
                print(' caching files locally in %s' % self._cache.path)
            return

        self.remote = False
        if not os.path.exists(path):
            print('WARNING: path does not resolve.  Archive will be non-functional.')
            self.remote = True
            self.compressed = False
            self._archive = None
            self.OK = False
            self._internal_subfolders = []
            self.ext = None
            return

        if os.path.isdir(path):
            # print('Path points to a directory. Assuming expanded archive')
            self.path = os.path.abspath(path) + os.path.sep  # abs reference plus trailing slash
            self.compressed = False
            self._archive = None
            self.OK = os.access(path, os.R_OK)
            self._internal_subfolders = [x[0][len(self.path):] for x in os.walk(path) if x[0] != path]
        else:
            self.compressed = True
            self.ext = get_ext(path)
            print('Found Extension: %s' % self.ext)
            self._archive = {
                '7z': self._access_7z,
                'zip': self._access_zip
            }[self.ext](path)
            self.OK = self._archive is not False
            if self.OK:
                self._internal_subfolders = {
                    '7z': self._int_dirs_7z,
                    'zip': self._int_dirs_zip
                }[self.ext]()

    @property
    def internal_prefix(self):
        return self._internal_prefix

    @internal_prefix.setter
    def internal_prefix(self, value):
        """
        incrementally add to prefix
        :param value:
        :return:
        """
        if self._internal_prefix is None:
            self._internal_prefix = value
        else:
            self._internal_prefix = self.pathtype.join(self._internal_prefix, value)

    @property
    def pathtype(self):
        if self.compressed or self.remote:
            return posixpath
        else:
            return os.path

    def _int_dirs_7z(self):
        s = set()
        for f in self._archive.files:
            s.add(os.path.split(f.filename)[0])
        return list(s)

    def _int_dirs_zip(self):
        s = set()
        for f in self._archive.namelist():
            s.add(os.path.split(f)[0])
        return list(s)

    def _prefix(self, file):
        if self._internal_prefix is None:
            return file
        return self.pathtype.join(self._internal_prefix, file)

    def _de_prefix(self, file):
        if self._internal_prefix is None:
            return file
        return re.sub('^' + self.pathtype.join(self._internal_prefix, ''), '', file)

    def _gen_files(self):
        """
        Generate files in the archive, removing the internal prefix
        :return:
        """
        if self.remote:
            raise AttributeError('Unable to list files for remote archives')

        if self.compressed:
            if self.ext == '7z':
                lg = (q.filename for q in self._archive.files)
            elif self.ext == 'zip':
                # filter out directory entries
                lg = (q for q in self._archive.namelist() if q[:-1] not in self._internal_subfolders)
            else:
                lg = []
        else:
            w = os.walk(self.path)
            lg = []
            for i in w:
                if len(i[2]) > 0:
                    prefix = i[0][len(self.path):]
                    lg.extend([os.path.join(prefix, z) for z in i[2]])
        for l in lg:
            yield self._de_prefix(l)

    def listfiles(self, in_prefix=''):
        """
        generate files in the archive, removing internal prefix, optionally filtering to specified prefix.
        :param in_prefix: optional prefix to limit
        :return:
        """
        if self.remote:
            if self.cache:
                g = (l for l in self._cache.listfiles(in_prefix=in_prefix))
            else:
                g = []
        else:
            g = self._gen_files()

        for l in g:
            if in_prefix is not None:
                if not bool(re.match('^' + in_prefix, l)):
                    continue
            yield l

    def writefile(self, fname, file, mode='wb'):
        if self.remote:
            print('Cannot write remote files.')
            return
        if self.compressed:
            print('Writing to compressed archives not supported.')
            return
        with open(os.path.join(self.path, fname), mode) as fp:
            fp.write(file)

    def readfile(self, fname, force_download=False):
        """
        Have to decide what this does. I think it should return the raw data- since there's no way to get a pointer
        to a file in a generic archive
        :param fname:
        :param force_download: for remote caching archives:
        :return:
        """
        if self.remote:
            if self.cache:
                if force_download:
                    print('Download forced')
                else:
                    try:
                        r = self._cache.readfile(fname)
                        print('Found file in cache: %s' % fname)
                        return r
                    except FileNotFoundError:
                        print('File not found in cache.. downloading')

            url = urljoin(self.path, fname)
            if self.query_string is not None:
                url += '?' + self.query_string
            print('Accessing remote url: %s' % url)
            file = {
                'http': lambda x: urlopen(x),
                None: None
            }[self.ext](url)
            if self.cache:
                self._create_cache_dir(os.path.dirname(url))
                self._cache.writefile(fname, file.read())
                return self._cache.readfile(fname)

            return file.read()

        elif self.compressed:
            file = {
                '7z': lambda x: self._archive.getmember(x),
                'zip': lambda x: self._archive.open(x)
            }[self.ext](self._prefix(fname))
            if file is None:
                return file
            else:
                return file.read()

        else:
            file = open(os.path.join(self.path, self._prefix(fname)), 'rb')
            data = file.read()
            file.close()
            return data
