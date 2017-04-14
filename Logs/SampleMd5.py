import hashlib

def md5Checksum(filePath):
    with open(filePath, 'rb') as fh:
        m = hashlib.md5()
        while True:
            data = fh.read(512)
            if not data:
                break
            m.update(data)
        return m.hexdigest()

print('The MD5 checksum of text.txt is', md5Checksum('log1.txt'))
#('The MD5 checksum of text.txt is', 'd1c2db2fafa31fe1a32ffde7a8c05c1b')