if (NOT DEBUG)
  string(APPEND CFLAGS " -O2")
endif()
if (NOT DEBUG)
  string(APPEND FFLAGS " -O2")
endif()
if (COMP_NAME STREQUAL gptl)
  string(APPEND CPPDEFS " -DHAVE_SLASHPROC")
endif()
if (MPILIB STREQUAL mpi-serial)
  string(APPEND SLIBS " -L$ENV{NETCDF_C_PATH}/lib -lnetcdf -L$ENV{NETCDF_FORTRAN_PATH}/lib -lnetcdff -L$ENV{ESSL_PATH}/lib64 -lessl -L$ENV{OLCF_NETLIB_LAPACK_ROOT}/lib -llapack")
endif()
if (NOT MPILIB STREQUAL mpi-serial)
  string(APPEND SLIBS " -L$ENV{PNETCDF_PATH}/lib -lpnetcdf -L$ENV{HDF5_PATH}/lib -lhdf5_hl -lhdf5 -L$ENV{NETCDF_C_PATH}/lib -lnetcdf -L$ENV{NETCDF_FORTRAN_PATH}/lib -lnetcdff -L$ENV{ESSL_PATH}/lib64 -lessl -L$ENV{OLCF_NETLIB_LAPACK_ROOT}/lib -llapack")
endif()
set(CXX_LIBS "-lstdc++")
set(MPICC "mpicc")
set(MPICXX "mpiCC")
set(MPIFC "mpif90")
set(SCC "gcc")
set(SCXX "g++")
set(SFC "gfortran")
set(PIO_FILESYSTEM_HINTS "gpfs")
set(NETCDF_C_PATH "$ENV{NETCDF_C_PATH}")
set(NETCDF_FORTRAN_PATH "$ENV{NETCDF_FORTRAN_PATH}")
set(PNETCDF_PATH "$ENV{PNETCDF_PATH}")
set(SUPPORTS_CXX "TRUE")
