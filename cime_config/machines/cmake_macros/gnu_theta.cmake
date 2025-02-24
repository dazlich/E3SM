if (NOT DEBUG)
  string(APPEND CFLAGS " -O2")
endif()
if (DEBUG)
  string(APPEND CFLAGS " -O0")
endif()
if (NOT DEBUG)
  string(APPEND FFLAGS " -O2")
endif()
if (DEBUG)
  string(APPEND FFLAGS " -O0")
endif()
execute_process(COMMAND nf-config --flibs OUTPUT_VARIABLE SHELL_CMD_OUTPUT_BUILD_INTERNAL_IGNORE0 OUTPUT_STRIP_TRAILING_WHITESPACE)
string(APPEND SLIBS " ${SHELL_CMD_OUTPUT_BUILD_INTERNAL_IGNORE0}")
set(CXX_LIBS "-lstdc++")
