import os

for i in range(20):
	os.system("ant test -q")
	os.system("ant systemtest -q")
