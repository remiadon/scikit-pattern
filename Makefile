install:
	pip install -r requirements.txt
	git clone https://github.com/andreasvc/roaringbitmap.git && cd roaringbitmap && python setup.py install

clean:
	$(RM) *.c *.so *.lprof *.html
