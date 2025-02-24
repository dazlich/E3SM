if (compile_threaded)
  string(APPEND CFLAGS " -h omp")
endif()
if (DEBUG)
  string(APPEND CFLAGS " -g -O0")
endif()
string(APPEND CPPDEFS " -DFORTRANUNDERSCORE -DNO_R16 -DCPRCRAY")
set(FC_AUTO_R8 "-s real64")
set(FFLAGS "-f free -N 255 -h byteswapio -em")
if (compile_threaded)
  string(APPEND FFLAGS " -h omp")
endif()
if (NOT compile_threaded)
  string(APPEND FFLAGS " -M1077")
endif()
if (DEBUG)
  string(APPEND FFLAGS " -g -O0")
endif()
set(FFLAGS_NOOPT "-O0")
set(HAS_F2008_CONTIGUOUS "TRUE")
set(LDFLAGS "-Wl,--allow-multiple-definition -h byteswapio")
if (compile_threaded)
  string(APPEND LDFLAGS " -h omp")
endif()
set(SUPPORTS_CXX "TRUE")
set(CXX_LINKER "FORTRAN")
