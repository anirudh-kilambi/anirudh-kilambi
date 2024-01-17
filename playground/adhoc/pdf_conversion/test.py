from io import StringIO

file = "in.pdf"

buf = StringIO(file)

print(buf.readline())


