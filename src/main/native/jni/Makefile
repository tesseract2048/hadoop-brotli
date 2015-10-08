#brotli/tools

include ../brotli/shared.mk

BROTLI = ../brotli
ENCOBJ = $(BROTLI)/enc/*.o
DECOBJ = $(BROTLI)/dec/*.o
EXE_OBJS=bro.o
CXXFLAGS += -I$(BROTLI)/tools -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/darwin -I$(JAVA_HOME)/include/linux -fPIC
all : libbrojni.so

libbrojni.so : $(EXE_OBJS) deps
	$(CXX) -shared -fwrapv -O3 $(EXE_OBJS) $(ENCOBJ) $(DECOBJ) -o $@

deps :
	$(MAKE) -C $(BROTLI)/dec
	$(MAKE) -C $(BROTLI)/enc

clean :
	rm -f $(OBJS) $(EXE_OBJS) $(EXECUTABLES)
	$(MAKE) -C $(BROTLI)/dec clean
	$(MAKE) -C $(BROTLI)/enc clean
