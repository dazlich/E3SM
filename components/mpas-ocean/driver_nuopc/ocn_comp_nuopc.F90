module ocn_comp_nuopc

  !----------------------------------------------------------------------------
  ! This is the NUOPC cap for MPAS-Ocean
  !----------------------------------------------------------------------------
  use ESMF
  use NUOPC                 , only : NUOPC_CompDerive, NUOPC_CompSetEntryPoint, NUOPC_CompSpecialize
  use NUOPC                 , only : NUOPC_CompFilterPhaseMap, NUOPC_IsUpdated, NUOPC_IsAtTime
  use NUOPC                 , only : NUOPC_CompAttributeGet, NUOPC_Advertise, NUOPC_CompSetClock
  use NUOPC                 , only : NUOPC_SetAttribute, NUOPC_CompAttributeGet, NUOPC_CompAttributeSet
  use NUOPC_Model           , only : model_routine_SS           => SetServices
  use NUOPC_Model           , only : SetVM
  use NUOPC_Model           , only : model_label_Advance        => label_Advance
  use NUOPC_Model           , only : model_label_DataInitialize => label_DataInitialize
  use NUOPC_Model           , only : model_label_SetRunClock    => label_SetRunClock
  use NUOPC_Model           , only : model_label_CheckImport    => label_CheckImport
  use NUOPC_Model           , only : model_label_SetClock       => label_SetClock
  use NUOPC_Model           , only : model_label_Finalize       => label_Finalize
  use NUOPC_Model           , only : NUOPC_ModelGet
  !use perf_mod              , only : t_startf, t_stopf
  use ocn_import_export     , only : ocn_advertise_fields, ocn_realize_fields
  use ocn_import_export     , only : ocn_import, ocn_export !, tlast_coupled
  use nuopc_shr_methods     , only : chkerr, state_setscalar, state_getscalar, state_diagnose, alarmInit
  use nuopc_shr_methods     , only : set_component_logging, get_component_instance, log_clock_advance
  use shr_kind_mod          , only : cl=>shr_kind_cl, cs=>shr_kind_cs, SHR_KIND_CX
  use shr_file_mod 
  use shr_sys_mod 
  use mpas_framework
  use mpas_derived_types
  use mpas_pool_routines
  use mpas_stream_manager
  use mpas_abort
  use ocn_core_interface

  ! !PUBLIC MEMBER FUNCTIONS:
  implicit none
  private                              ! By default make data private

  public  :: SetServices
  public  :: SetVM
  private :: InitializeP0
  private :: InitializeAdvertise
  private :: InitializeRealize
  private :: ModelAdvance
  private :: ModelSetRunClock
  private :: ModelFinalize

  integer, parameter  :: dbug = 1
  character(*), parameter :: u_FILE_u = &
       __FILE__

  integer           :: lmpicom
  integer           :: stdout 
  integer           :: nThreads        ! number of threads per mpi task for this component
  character(len=CL)   :: flds_scalar_name = ''
  integer             :: flds_scalar_num = 0
  integer             :: flds_scalar_index_nx = 0
  integer             :: flds_scalar_index_ny = 0

  type (core_type), pointer :: corelist => null()
  type (dm_info), pointer :: dminfo
  type (domain_type), pointer :: domain_ptr

!=======================================================================
contains
!=======================================================================

  subroutine SetServices(gcomp, rc)

    ! Arguments
    type(ESMF_GridComp)  :: gcomp
    integer, intent(out) :: rc

    ! Local variables
    character(len=*),parameter  :: subname='ocn_comp_nuopc:(SetServices) '
    !--------------------------------

    rc = ESMF_SUCCESS
    if (dbug > 5) call ESMF_LogWrite(subname//' called', ESMF_LOGMSG_INFO)

    ! the NUOPC gcomp component will register the generic methods
    call NUOPC_CompDerive(gcomp, model_routine_SS, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    ! switching to IPD versions
    call ESMF_GridCompSetEntryPoint(gcomp, ESMF_METHOD_INITIALIZE, &
         userRoutine=InitializeP0, phase=0, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    ! set entry point for methods that require specific implementation
    call NUOPC_CompSetEntryPoint(gcomp, ESMF_METHOD_INITIALIZE, &
         phaseLabelList=(/"IPDv01p1"/), userRoutine=InitializeAdvertise, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    call NUOPC_CompSetEntryPoint(gcomp, ESMF_METHOD_INITIALIZE, &
         phaseLabelList=(/"IPDv01p3"/), userRoutine=InitializeRealize, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    ! attach specializing method(s)
    call NUOPC_CompSpecialize(gcomp, specLabel=model_label_DataInitialize, &
         specRoutine=DataInitialize, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    call NUOPC_CompSpecialize(gcomp, specLabel=model_label_Advance, &
         specRoutine=ModelAdvance, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    call ESMF_MethodRemove(gcomp, label=model_label_SetRunClock, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    call NUOPC_CompSpecialize(gcomp, specLabel=model_label_SetRunClock, &
         specRoutine=ModelSetRunClock, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    call ESMF_MethodRemove(gcomp, label=model_label_CheckImport, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    call NUOPC_CompSpecialize(gcomp, specLabel=model_label_CheckImport, &
         specRoutine=ModelCheckImport, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    call NUOPC_CompSpecialize(gcomp, specLabel=model_label_Finalize, &
         specRoutine=ModelFinalize, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    if (dbug > 5) call ESMF_LogWrite(subname//' done', ESMF_LOGMSG_INFO)

  end subroutine SetServices

  !===============================================================================

  subroutine InitializeP0(gcomp, importState, exportState, clock, rc)

    ! Arguments
    type(ESMF_GridComp)   :: gcomp
    type(ESMF_State)      :: importState, exportState
    type(ESMF_Clock)      :: clock
    integer, intent(out)  :: rc
    !--------------------------------

    rc = ESMF_SUCCESS

    ! Switch to IPDv01 by filtering all other phaseMap entries
    call NUOPC_CompFilterPhaseMap(gcomp, ESMF_METHOD_INITIALIZE, &
         acceptStringList=(/"IPDv01p"/), rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

  end subroutine InitializeP0

  !===============================================================================

  subroutine InitializeAdvertise(gcomp, importState, exportState, clock, rc)

    use NUOPC, only : NUOPC_isConnected

    ! input/output variables
    type(ESMF_GridComp)  :: gcomp
    type(ESMF_State)     :: importState, exportState
    type(ESMF_Clock)     :: clock
    integer, intent(out) :: rc

    ! local variables
    type(ESMF_VM)     :: vm
    integer           :: iam
    integer           :: shrlogunit
    character(len=CL) :: logmsg
    character(len=CS) :: cvalue
    logical           :: isPresent, isSet
    character(len=*), parameter :: subname='ocn_comp_nuopc:(InitializeAdvertise) '
    !--------------------------------

    call ESMF_GridCompGet(gcomp, vm=vm, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    call ESMF_VMGet(vm, mpiCommunicator=lmpicom, localPet=iam, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    ! reset shr logging to my log file
    call set_component_logging(gcomp, iam==0, stdout, shrlogunit, rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    call NUOPC_CompAttributeGet(gcomp, name="ScalarFieldName", value=cvalue, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    flds_scalar_name = trim(cvalue)
    call ESMF_LogWrite(trim(subname)//' flds_scalar_name = '//trim(flds_scalar_name), ESMF_LOGMSG_INFO)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    call NUOPC_CompAttributeGet(gcomp, name="ScalarFieldCount", value=cvalue, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    read(cvalue, *) flds_scalar_num
    write(logmsg,*) flds_scalar_num
    call ESMF_LogWrite(trim(subname)//' flds_scalar_num = '//trim(logmsg), ESMF_LOGMSG_INFO)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    call NUOPC_CompAttributeGet(gcomp, name="ScalarFieldIdxGridNX", value=cvalue, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    read(cvalue,*) flds_scalar_index_nx
    write(logmsg,*) flds_scalar_index_nx
    call ESMF_LogWrite(trim(subname)//' : flds_scalar_index_nx = '//trim(logmsg), ESMF_LOGMSG_INFO)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    call NUOPC_CompAttributeGet(gcomp, name="ScalarFieldIdxGridNY", value=cvalue, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    read(cvalue,*) flds_scalar_index_ny
    write(logmsg,*) flds_scalar_index_ny
    call ESMF_LogWrite(trim(subname)//' : flds_scalar_index_ny = '//trim(logmsg), ESMF_LOGMSG_INFO)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    ! Advertise fields
    call ocn_advertise_fields(gcomp, importState, exportState, flds_scalar_name, rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    
  end subroutine InitializeAdvertise

  !===============================================================================

  subroutine InitializeRealize(gcomp, importState, exportState, clock, rc)

    !-----------------------------------------------------------------------
    !  first initializaiton phase of pop2
    !  initialize the timers, communication routines, global reductions,
    !  domain decomposition, grid, and overflows
    !-----------------------------------------------------------------------
    use ESMF               , only: ESMF_VMGet
      use mpas_stream_manager, only : MPAS_stream_mgr_init, MPAS_build_stream_filename, MPAS_stream_mgr_validate_streams
      use iso_c_binding, only : c_char, c_loc, c_ptr, c_int
      use mpas_c_interfacing, only : mpas_f_to_c_string, mpas_c_to_f_string
      use mpas_timekeeping, only : mpas_get_clock_time, mpas_get_time
      use mpas_bootstrapping, only : mpas_bootstrap_framework_phase1, mpas_bootstrap_framework_phase2
      use mpas_log


    ! Initialize POP

    ! input/output variables
    type(ESMF_GridComp)  :: gcomp
    type(ESMF_State)     :: importState
    type(ESMF_State)     :: exportState
    type(ESMF_Clock)     :: clock
    integer, intent(out) :: rc

    !  local variables
    integer                 :: iblock
    type(ESMF_VM)           :: vm
    type(ESMF_DistGrid)     :: distGrid
    type(ESMF_Mesh)         :: Emesh
    integer , allocatable   :: gindex_ocn(:)
    integer , allocatable   :: gindex_elim(:)
    integer , allocatable   :: gindex(:)
    integer                 :: globalID
    character(CL)           :: cvalue
    integer                 :: num_elim_global
    integer                 :: num_elim_local
    integer                 :: num_elim
    integer                 :: num_ocn
    integer                 :: num_elim_gcells ! local number of eliminated gridcells
    integer                 :: num_elim_blocks ! local number of eliminated blocks
    integer                 :: num_total_blocks
    integer                 :: my_elim_start
    integer                 :: my_elim_end
    integer                 :: lsize
    integer                 :: shrlogunit      ! old values
    integer                 :: npes
    integer                 :: iam
    logical                 :: mastertask
    character(len=32)       :: starttype
    integer                 :: n,i,j,iblk,jblk,ig,jg,ierr
    integer                 :: lbnum
    integer                 :: errorCode       ! error code
    character(len=*), parameter  :: subname = "ocn_comp_nuopc:(InitializeRealize)"
    integer, dimension(:), pointer :: indexToCellID, indextocellid_0halo
    logical :: readNamelistArg, readStreamsArg
    character(len=SHR_KIND_CX) :: argument, namelistFile, streamsFile
    type (block_type), pointer :: block
    integer :: iCell, nCells, ncells_0halo
    integer, dimension(:), pointer :: nCellsArray
    type (mpas_pool_type), pointer :: meshPool

      character(len=StrKIND) :: iotype
      logical :: streamsExists
      integer :: mesh_iotype
      type (c_ptr) :: mgr_p

      character(len=StrKIND) :: mesh_stream
      character(len=StrKIND) :: mesh_filename
      character(len=StrKIND) :: mesh_filename_temp
      character(len=StrKIND) :: ref_time_temp
      character(len=StrKIND) :: filename_interval_temp
      character(kind=c_char), dimension(StrKIND+1) :: c_mesh_stream
      character(kind=c_char), dimension(StrKIND+1) :: c_mesh_filename_temp
      character(kind=c_char), dimension(StrKIND+1) :: c_ref_time_temp
      character(kind=c_char), dimension(StrKIND+1) :: c_filename_interval_temp
      character(kind=c_char), dimension(StrKIND+1) :: c_iotype

      integer :: blockID

      character(kind=c_char), dimension(StrKIND+1) :: c_filename       ! StrKIND+1 for C null-termination character
      integer(kind=c_int) :: c_comm
      integer(kind=c_int) :: c_ierr

      type (MPAS_Time_type) :: start_time
      type (MPAS_Time_type) :: ref_time
      type (MPAS_TimeInterval_type) :: filename_interval
      character(len=StrKIND) :: timeStamp

      interface
         subroutine xml_stream_parser(xmlname, mgr_p, comm, ierr) bind(c)
            use iso_c_binding, only : c_char, c_ptr, c_int
            character(kind=c_char), dimension(*), intent(in) :: xmlname
            type (c_ptr), intent(inout) :: mgr_p
            integer(kind=c_int), intent(inout) :: comm
            integer(kind=c_int), intent(out) :: ierr
         end subroutine xml_stream_parser

         subroutine xml_stream_get_attributes(xmlname, streamname, comm, filename, ref_time, filename_interval, io_type, ierr) bind(c)
            use iso_c_binding, only : c_char, c_int
            character(kind=c_char), dimension(*), intent(in) :: xmlname
            character(kind=c_char), dimension(*), intent(in) :: streamname
            integer(kind=c_int), intent(inout) :: comm
            character(kind=c_char), dimension(*), intent(out) :: filename
            character(kind=c_char), dimension(*), intent(out) :: ref_time
            character(kind=c_char), dimension(*), intent(out) :: filename_interval
            character(kind=c_char), dimension(*), intent(out) :: io_type
            integer(kind=c_int), intent(out) :: ierr
         end subroutine xml_stream_get_attributes
      end interface

    !-----------------------------------------------------------------------

    rc = ESMF_SUCCESS
    errorCode = ESMF_SUCCESS

    if (dbug > 5) call ESMF_LogWrite(subname//' called', ESMF_LOGMSG_INFO)


    call ESMF_GridCompGet(gcomp, vm=vm, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    call ESMF_VMGet(vm, localPet=iam, PetCount=npes, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    call ESMF_VMGet(vm, pet=iam, peCount=nthreads, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    if(nthreads==1) then
       call NUOPC_CompAttributeGet(gcomp, "nthreads", value=cvalue, rc=rc)
       if (ESMF_LogFoundError(rcToCheck=rc, msg=ESMF_LOGERR_PASSTHRU, line=__LINE__, file=u_FILE_u)) return
       read(cvalue,*) nthreads
    endif

!?!$  call omp_set_num_threads(nThreads)

#if (defined _MEMTRACE)
    if (iam == 0) then
       lbnum=1
       call memmon_dump_fort('memmon.out','InitializeRealize:start::',lbnum)
    endif
#endif

    !-----------------------------------------------------------------------
    ! initialize the model run
    !-----------------------------------------------------------------------

    !?call NUOPC_CompAttributeGet(gcomp, name='case_name', value=cvalue, rc=rc)
    !?if (ChkErr(rc,__LINE__,u_FILE_u)) return
    !?read(cvalue,*) runid

    !?call NUOPC_CompAttributeGet(gcomp, name='start_type', value=cvalue, rc=rc)
    !?if (ChkErr(rc,__LINE__,u_FILE_u)) return
    !?read(cvalue,*) starttype

    !?if (trim(starttype) == trim('startup')) then
    !?   runtype = "initial"
    !?else if (trim(starttype) == trim('continue') ) then
    !?   runtype = "continue"
    !?else if (trim(starttype) == trim('branch')) then
    !?   runtype = "continue"
    !?else
    !?   write(stdout,*) 'ocn_comp_nuopc: ERROR: unknown starttype'
!?  !?     call exit_POP(sigAbort,' ocn_comp_nuopc: ERROR: unknown starttype')
    !?end if

    ! TODO: Set model_doi_url

    !?call get_component_instance(gcomp, inst_suffix, inst_index, rc)
    !?if (ChkErr(rc,__LINE__,u_FILE_u)) return
    !?inst_name = "OCN"

    !-----------------------------------------------------------------------
    !  first initializaiton phase of mpas-ocean
    !  initialize the timers, communication routines, global reductions,
    !  domain decomposition, grid, and overflows
    !-----------------------------------------------------------------------

    !?call t_startf ('mpaso_init1')

      readNamelistArg = .false.
      readStreamsArg = .false.
      !?!? do I need what follow in mpas_subdriver/mpas_init?
!---the rest of this routine was copied from mpas_subdriver.F, subroutine mpas_init
    allocate(corelist)
    nullify(corelist % next)

    allocate(corelist % domainlist)
    nullify(corelist % domainlist % next)

    domain_ptr => corelist % domainlist
    domain_ptr % core => corelist

    call mpas_allocate_domain(domain_ptr)

    !
    ! Initialize infrastructure
    !
    call mpas_framework_init_phase1(domain_ptr % dminfo, lmpicom)
    
      call ocn_setup_core(corelist)
      call ocn_setup_domain(domain_ptr)

      ! Set up the log manager as early as possible so we can use it for any errors/messages during subsequent init steps
      ! We need:
      ! 1) domain_ptr to be allocated,
      ! 2) dmpar_init complete to access dminfo,
      ! 3) *_setup_core to assign the setup_log function pointer
      ierr = domain_ptr % core % setup_log(domain_ptr % logInfo, domain_ptr)
      if ( ierr /= 0 ) then
         call mpas_dmpar_global_abort('ERROR: Log setup failed for core ' // trim(domain_ptr % core % coreName))
      end if

      if ( readNamelistArg ) then
         domain_ptr % namelist_filename = namelistFile
      end if

      if ( readStreamsArg ) then
         domain_ptr % streams_filename = streamsFile
      end if

      !DD hardwire for now
      domain_ptr % namelist_filename = 'mpaso_in'
      domain_ptr % streams_filename = 'streams.ocean'
      ierr = domain_ptr % core % setup_namelist(domain_ptr % configs, domain_ptr % namelist_filename, domain_ptr % dminfo)
      if ( ierr /= 0 ) then
         call mpas_log_write('Namelist setup failed for core '//trim(domain_ptr % core % coreName), messageType=MPAS_LOG_CRIT)
      end if
    if (errorCode /= ESMF_Success) then
       call ESMF_LogWrite(trim(subname)//'MPASO_initialize1: error in mpaso_init_phase1',ESMF_LOGMSG_INFO, rc=rc)
       rc = ESMF_FAILURE
       return
    endif

    !?call t_stopf ('mpaso_init1')

      call mpas_framework_init_phase2(domain_ptr)

      ierr = domain_ptr % core % define_packages(domain_ptr % packages)
      if ( ierr /= 0 ) then
         call mpas_log_write('Package definition failed for core '//trim(domain_ptr % core % coreName), messageType=MPAS_LOG_CRIT)
      end if

      ierr = domain_ptr % core % setup_packages(domain_ptr % configs, domain_ptr % packages, domain_ptr % iocontext)
      if ( ierr /= 0 ) then
         call mpas_log_write('Package setup failed for core '//trim(domain_ptr % core % coreName), messageType=MPAS_LOG_CRIT)
      end if

      ierr = domain_ptr % core % setup_decompositions(domain_ptr % decompositions)
      if ( ierr /= 0 ) then
         call mpas_log_write('Decomposition setup failed for core '//trim(domain_ptr % core % coreName), messageType=MPAS_LOG_CRIT)
      end if

      ierr = domain_ptr % core % setup_clock(domain_ptr % clock, domain_ptr % configs)
      if ( ierr /= 0 ) then
         call mpas_log_write('Clock setup failed for core '//trim(domain_ptr % core % coreName), messageType=MPAS_LOG_CRIT)
      end if

      call mpas_log_write('Reading streams configuration from file '//trim(domain_ptr % streams_filename))
      inquire(file=trim(domain_ptr % streams_filename), exist=streamsExists)

      if ( .not. streamsExists ) then
         call mpas_log_write('Streams file '//trim(domain_ptr % streams_filename)//' does not exist.', messageType=MPAS_LOG_CRIT)
      end if

      call mpas_timer_start('total time')
      call mpas_timer_start('initialize')

      !
      ! Using information from the namelist, a graph.info file, and a file containing
      !    mesh fields, build halos and allocate blocks in the domain
      !
      ierr = domain_ptr % core % get_mesh_stream(domain_ptr % configs, mesh_stream)
      if ( ierr /= 0 ) then
         call mpas_log_write('Failed to find mesh stream for core '//trim(domain_ptr % core % coreName), messageType=MPAS_LOG_CRIT)
      end if

      call mpas_f_to_c_string(domain_ptr % streams_filename, c_filename)
      call mpas_f_to_c_string(mesh_stream, c_mesh_stream)
      c_comm = domain_ptr % dminfo % comm
      call xml_stream_get_attributes(c_filename, c_mesh_stream, c_comm, &
                                     c_mesh_filename_temp, c_ref_time_temp, &
                                     c_filename_interval_temp, c_iotype, c_ierr)
      if (c_ierr /= 0) then
         call mpas_log_write('stream xml get attribute failed: '//trim(domain_ptr % streams_filename), messageType=MPAS_LOG_CRIT)
      end if
      call mpas_c_to_f_string(c_mesh_filename_temp, mesh_filename_temp)
      call mpas_c_to_f_string(c_ref_time_temp, ref_time_temp)
      call mpas_c_to_f_string(c_filename_interval_temp, filename_interval_temp)
      call mpas_c_to_f_string(c_iotype, iotype)

      if (trim(iotype) == 'pnetcdf') then
         mesh_iotype = MPAS_IO_PNETCDF
      else if (trim(iotype) == 'pnetcdf,cdf5') then
         mesh_iotype = MPAS_IO_PNETCDF5
      else if (trim(iotype) == 'netcdf') then
         mesh_iotype = MPAS_IO_NETCDF
      else if (trim(iotype) == 'netcdf4') then
         mesh_iotype = MPAS_IO_NETCDF4
      else
         mesh_iotype = MPAS_IO_PNETCDF
      end if

      start_time = mpas_get_clock_time(domain_ptr % clock, MPAS_START_TIME, ierr)
      if ( trim(ref_time_temp) == 'initial_time' ) then
          call mpas_get_time(start_time, dateTimeString=ref_time_temp, ierr=ierr)
      end if

      blockID = -1
      if ( trim(filename_interval_temp) == 'none' ) then
          call mpas_expand_string(ref_time_temp, blockID, mesh_filename_temp, mesh_filename)
      else
          call mpas_set_time(ref_time, dateTimeString=ref_time_temp, ierr=ierr)
          call mpas_set_timeInterval(filename_interval, timeString=filename_interval_temp, ierr=ierr)
          call mpas_build_stream_filename(ref_time, start_time, filename_interval, mesh_filename_temp, blockID, mesh_filename, ierr)
      end if
      call mpas_log_write(' ** Attempting to bootstrap MPAS framework using stream: ' // trim(mesh_stream))
      call mpas_bootstrap_framework_phase1(domain_ptr, mesh_filename, mesh_iotype)

      !
      ! Set up run-time streams
      !
      call MPAS_stream_mgr_init(domain_ptr % streamManager, domain_ptr % ioContext, domain_ptr % clock, &
                                domain_ptr % blocklist % allFields, domain_ptr % packages, domain_ptr % blocklist % allStructs)

      call add_stream_attributes(domain_ptr)

      ierr = domain_ptr % core % setup_immutable_streams(domain_ptr % streamManager)
      if ( ierr /= 0 ) then
         call mpas_log_write('Immutable streams setup failed for core '//trim(domain_ptr % core % coreName), messageType=MPAS_LOG_CRIT)
      end if

      mgr_p = c_loc(domain_ptr % streamManager)
      call xml_stream_parser(c_filename, mgr_p, c_comm, c_ierr)
      if (c_ierr /= 0) then
         call mpas_log_write('xml stream parser failed: '//trim(domain_ptr % streams_filename), messageType=MPAS_LOG_CRIT)
      end if

      !
      ! Validate streams after set-up
      !
      call mpas_log_write(' ** Validating streams')
      call MPAS_stream_mgr_validate_streams(domain_ptr % streamManager, ierr = ierr)
      if ( ierr /= MPAS_STREAM_MGR_NOERR ) then
         call mpas_dmpar_global_abort('ERROR: Validation of streams failed for core ' // trim(domain_ptr % core % coreName))
      end if

      !
      ! Finalize the setup of blocks and fields
      !
      call mpas_bootstrap_framework_phase2(domain_ptr)

      !
      ! Initialize core
      !
      iErr = domain_ptr % core % core_init(domain_ptr, timeStamp)
      if ( ierr /= 0 ) then
         call mpas_log_write('Core init failed for core '//trim(domain_ptr % core % coreName), messageType=MPAS_LOG_CRIT)
      end if

    !---------------------------------------------------------------------------
    ! Determine the global index space needed for the distgrid
    !---------------------------------------------------------------------------

    n = 0
    block => domain_ptr % blocklist
    do while (associated(block))
       call mpas_pool_get_subpool(block % structs, 'mesh', meshPool)
       call mpas_pool_get_dimension(meshPool, 'nCellsArray', nCellsArray)
       nCells = nCellsArray( 1 )
       n = n + ncells
       block => block % next
    end do
    lsize = n
    allocate(gindex_ocn(lsize))

    n = 0
    block => domain_ptr % blocklist
    do while (associated(block))
       call mpas_pool_get_subpool(block % structs, 'mesh', meshPool)
       call mpas_pool_get_dimension(meshPool, 'nCellsArray', nCellsArray)
       call mpas_pool_get_array(meshPool, 'indexToCellID', indexToCellID)
       nCells = nCellsArray( 1 )
       do iCell = 1, nCells
          gindex_ocn(n+iCell) =indexToCellID(iCell)
       enddo
       n = n + ncells
       block => block % next
    end do

!       ! No eliminated land blocks
       num_ocn = size(gindex_ocn)
       allocate(gindex(num_ocn))
       do n = 1,num_ocn
          gindex(n) = gindex_ocn(n)
       end do

    !---------------------------------------------------------------------------
    ! Create distGrid from global index array
    !---------------------------------------------------------------------------
    DistGrid = ESMF_DistGridCreate(arbSeqIndexList=gindex, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    !---------------------------------------------------------------------------
    ! Create the MPAS-O mesh
    !---------------------------------------------------------------------------

    ! read in the mesh
    call NUOPC_CompAttributeGet(gcomp, name='mesh_ocn', value=cvalue, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    !EMesh = ESMF_MeshCreate(filename=trim(cvalue), fileformat=ESMF_FILEFORMAT_ESMFMESH, &
    EMesh = ESMF_MeshCreate(filename=trim(cvalue), fileformat=ESMF_FILEFORMAT_ESMFMESH, &
         elementDistgrid=Distgrid, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    mastertask = iam == domain_ptr % dminfo % my_proc_id
    if (mastertask) then
       write(stdout,*)'mesh file for mpaso domain is ',trim(cvalue)
    end if

    !-----------------------------------------------------------------
    ! Realize the actively coupled fields
    !-----------------------------------------------------------------

    call ocn_realize_fields(gcomp, mesh=Emesh, flds_scalar_name=flds_scalar_name, &
         flds_scalar_num=flds_scalar_num, mastertask = mastertask, lmpicom = lmpicom,           &
         domain = domain_ptr, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

  end subroutine InitializeRealize


  !===============================================================================

  subroutine DataInitialize(gcomp, rc)

    !-----------------------------------------------------------------------
    !  second initializaiton phase of mpaso
    !?!  - initialize ???
    !-----------------------------------------------------------------------

    ! input/output variables
    type(ESMF_GridComp)  :: gcomp
    integer, intent(out) :: rc

    ! local variables
    type(ESMF_Clock)          :: clock
    type(ESMF_State)          :: importState
    type(ESMF_State)          :: exportState
    type(ESMF_StateItem_Flag) :: itemType
    type(ESMF_StateItem_Flag) :: itemType1
    type(ESMF_StateItem_Flag) :: itemType2
    type(ESMF_TimeInterval)   :: timeStep        ! Model timestep
    type(ESMF_Time)           :: starttime
    character(CL)             :: cvalue
    integer                   :: ocn_cpl_dt
    integer                   :: pop_cpl_dt
    integer                   :: start_ymd
    integer                   :: start_tod
    integer                   :: start_year
    integer                   :: start_day
    integer                   :: start_month
    integer                   :: start_hour
    integer                   :: errorCode       ! error code
    integer                   :: shrlogunit      ! old values
    integer                   :: ocnid
    character(len=*), parameter  :: subname = "ocn_comp_nuopc:(DataInitialize)"
    !-----------------------------------------------------------------------

    rc = ESMF_SUCCESS

    !--------------------------------
    ! Reset shr logging to my log file
    !--------------------------------

    call shr_file_getLogUnit (shrlogunit)
    call shr_file_setLogUnit (stdout)

    !-----------------------------------------------------------------------
    ! register non-standard incoming fields
    !-----------------------------------------------------------------------

    ! query the Component for its importState, exportState and clock
    call ESMF_GridCompGet(gcomp, importState=importState, exportState=exportState, clock=clock, rc=rc)

    !?call ESMF_StateGet(importState, 'Sa_co2prog', itemType, rc=rc)
    !?if (ChkErr(rc,__LINE__,u_FILE_u)) return
    !?ldriver_has_atm_co2_prog = (itemType /= ESMF_STATEITEM_NOTFOUND)

    !?call ESMF_StateGet(importState, 'Sa_co2diag', itemType, rc=rc)
    !?if (ChkErr(rc,__LINE__,u_FILE_u)) return
    !?ldriver_has_atm_co2_diag = (itemType /= ESMF_STATEITEM_NOTFOUND)

    !?call ESMF_StateGet(importState, 'Faxa_nhx', itemType1, rc=rc)
    !?if (ChkErr(rc,__LINE__,u_FILE_u)) return
    !?call ESMF_StateGet(importState, 'Faxa_noy', itemType2, rc=rc)
    !?if (ChkErr(rc,__LINE__,u_FILE_u)) return
    !?ldriver_has_ndep = ((itemType1 /= ESMF_STATEITEM_NOTFOUND) .or. (itemType2 /= ESMF_STATEITEM_NOTFOUND))

    !?if (ldriver_has_atm_co2_prog) then
    !?   call named_field_register('ATM_CO2_PROG', ATM_CO2_PROG_nf_ind)
    !?endif
    !?if (ldriver_has_atm_co2_diag) then
    !?   call named_field_register('ATM_CO2_DIAG', ATM_CO2_DIAG_nf_ind)
    !?endif
    !?if (ldriver_has_ndep) then
    !?   if (mastertask) write(stdout,'(" using ATM_NHx and ATM_NOy from mediator")')
    !?   call named_field_register('ATM_NHx', ATM_NHx_nf_ind)
    !?   call named_field_register('ATM_NOy', ATM_NOy_nf_ind)
    !?endif

    !?call register_string('pop_init_coupled')
    !?call flushm (stdout)

    !-----------------------------------------------------------------------
    ! second initialization phase of mpaso
    !-----------------------------------------------------------------------

    !?call t_startf ('mpaso_init2')

    !?call pop_init_phase2(errorCode)
    if (rc /= ESMF_Success) then
       call ESMF_LogWrite(trim(subname)//'MPASO_Initialize2: error in mpas_init_phase2',ESMF_LOGMSG_INFO, rc=rc)
       rc = ESMF_FAILURE
       return
    endif
    !?! initialize driver-level flags and timers
    !?call access_time_flag ('stop_now', stop_now)
    !?call access_time_flag ('coupled_ts', cpl_ts)
    !?call get_timer(timer_total,'TOTAL', 1, distrb_clinic%nprocs)

    !?call t_stopf ('mpaso_init2')

    !-----------------------------------------------------------------------
    !  initialize time-stamp information
    !-----------------------------------------------------------------------

    !?call ccsm_char_date_and_time

    !?!-----------------------------------------------------------------------
    !?! check for consistency of pop and sync clock initial time
    !?!-----------------------------------------------------------------------

    !?if (runtype == 'initial') then

    !?   call ESMF_ClockGet( clock, startTime=startTime, rc=rc )
    !?   if (ChkErr(rc,__LINE__,u_FILE_u)) return

    !?   call ESMF_TimeGet( startTime, yy=start_year, mm=start_month, dd=start_day, s=start_tod, rc=rc )
    !?   if (ChkErr(rc,__LINE__,u_FILE_u)) return

    !?   call shr_cal_ymd2date(start_year,start_month,start_day,start_ymd)

!?!$OMP MASTER
    !?   if (iyear0 /= start_year) then
    !?      call document ('DataInitialize', 'iyear0      ', iyear0)
    !?      call document ('DataInitialize', 'imonth0     ', imonth0)
    !?      call document ('DataInitialize', 'iday0       ', iday0)
    !?      call document ('DataInitialize', 'start_year  ', start_year)
    !?      call document ('DataInitialize', 'start_month ', start_month)
    !?      call document ('DataInitialize', 'start_day   ', start_day)

    !?      ! skip exit_POP if pop is 1 day ahead across a year boundary
    !?      if (.not. (iyear0 == start_year + 1 .and. &
    !?                (imonth0 == 1 .and. start_month == 12) .and. &
    !?                (iday0 == 1   .and. start_day == 31))) then
    !?         call exit_POP(sigAbort,' iyear0 does not match start_year')
    !?      endif
    !?   else if (imonth0 /= start_month) then
    !?      call document ('DataInitialize', 'imonth0     ', imonth0)
    !?      call document ('DataInitialize', 'iday0       ', iday0)
    !?      call document ('DataInitialize', 'start_month ', start_month)
    !?      call document ('DataInitialize', 'start_day   ', start_day)
    !?      ! skip exit_POP if pop is 1 day ahead across a month boundary
    !?      !   this conditional doesn't confirm that start_day is the last day of the month,
    !?      !   only that iday0 is the first day of the month
    !?      if (.not. (imonth0 == start_month + 1 .and. iday0 == 1)) then
    !?         call exit_POP(sigAbort,' imonth0 does not match start_month')
    !?      endif

    !?   else if (iday0 /= start_day) then
    !?      call document ('DataInitialize', 'iday0       ', iday0)
    !?      call document ('DataInitialize', 'start_day   ', start_day)

    !?      ! skip exit_POP if pop is 1 day ahead
    !?      if (.not. (iday0 == start_day + 1)) then
    !?         call exit_POP(sigAbort,' iday0 does not match start_day')
    !?      endif
    !?   end if
!?!$OMP END MASTER
    !?end if
    
    !?!-----------------------------------------------------------------
    !?! Initialize MCT gsmaps and domains
    !?!-----------------------------------------------------------------

    !?call NUOPC_CompAttributeGet(gcomp, name='MCTID', value=cvalue, rc=rc)
    !?if (ChkErr(rc,__LINE__,u_FILE_u)) return
    !?read(cvalue,*) ocnid  ! convert from string to integer

    !?call pop_mct_init(ocnid, mpi_communicator_ocn)
    !?if (dbug > 5) call ESMF_LogWrite(subname//' done', ESMF_LOGMSG_INFO)

    !?!-----------------------------------------------------------------------
    !?! Initialize flags and shortwave absorption profile
    !?! Note that these cpl_write_xxx flags have no freqency options
    !?! set; therefore, they will retain a default value of .false.
    !?! unless they are explicitly set .true.  at the appropriate times
    !?!-----------------------------------------------------------------------

    !?call init_time_flag('cpl_write_restart',cpl_write_restart, owner = 'DataInitialize')
    !?call init_time_flag('cpl_write_history',cpl_write_history, owner = 'DataInitialize')
    !?call init_time_flag('cpl_write_tavg'   ,cpl_write_tavg,    owner = 'DataInitialize')
    !?call init_time_flag('cpl_diag_global'  ,cpl_diag_global,   owner = 'DataInitialize')
    !?call init_time_flag('cpl_diag_transp'  ,cpl_diag_transp,   owner = 'DataInitialize')

    !?lsmft_avail = .true.
    !?tlast_coupled = c0

    !?!-----------------------------------------------------------------------
    !?! initialize necessary coupling info
    !?!-----------------------------------------------------------------------

    !?call ESMF_ClockGet(clock, timeStep=timeStep, rc=rc)
    !?if (ChkErr(rc,__LINE__,u_FILE_u)) return

    !?call ESMF_TimeIntervalGet( timeStep, s=ocn_cpl_dt, rc=rc )
    !?if (ChkErr(rc,__LINE__,u_FILE_u)) return

    !?pop_cpl_dt = seconds_in_day / ncouple_per_day

    !?if (pop_cpl_dt /= ocn_cpl_dt) then
    !?   write(stdout,*)'pop_cpl_dt= ',pop_cpl_dt,' ocn_cpl_dt= ',ocn_cpl_dt
    !?   call exit_POP(sigAbort,'ERROR pop_cpl_dt and ocn_cpl_dt must be identical')
    !?end if

    !-----------------------------------------------------------------------
    ! send export state
    !-----------------------------------------------------------------------

    call ocn_export(exportState, flds_scalar_name, domain_ptr,     &
                    errorCode, rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    !?if (errorCode /= POP_Success) then
    !?   call POP_ErrorPrint(errorCode)
    !?   call exit_POP(sigAbort, 'ERROR in ocn_export')
    !?endif

    !?call State_SetScalar(dble(nx_global), flds_scalar_index_nx, exportState, &
    !?     flds_scalar_name, flds_scalar_num, rc)
    !?if (ChkErr(rc,__LINE__,u_FILE_u)) return

    !?call State_SetScalar(dble(ny_global), flds_scalar_index_ny, exportState, &
    !?     flds_scalar_name, flds_scalar_num, rc)
    !?if (ChkErr(rc,__LINE__,u_FILE_u)) return

    !?if ( lsend_precip_fact ) then
    !?   call State_SetScalar(precip_fact, flds_scalar_index_precip_factor, exportState, &
    !?        flds_scalar_name, flds_scalar_num, rc)
    !?   if (ChkErr(rc,__LINE__,u_FILE_u)) return
    !?else
    !?   call State_SetScalar(1._r8, flds_scalar_index_precip_factor, exportState, &
    !?        flds_scalar_name, flds_scalar_num, rc)
    !?   if (ChkErr(rc,__LINE__,u_FILE_u)) return
    !?end if

#if (defined _MEMTRACE)
    if (iam  == 0) then
       lbnum=1
       call memmon_dump_fort('memmon.out','DataInitialize:end::',lbnum)
       call memmon_reset_addr()
    endif
#endif

    !?!-----------------------------------------------------------------------
    !?! Document orbital parameters
    !?!-----------------------------------------------------------------------

    !?if (registry_match('qsw_distrb_iopt_cosz')) then

    !?   call pop_orbital_init(gcomp, stdout, mastertask, rc)
    !?   if (ChkErr(rc,__LINE__,u_FILE_u)) return

    !?   call pop_orbital_update(clock, stdout, mastertask, orb_eccen, orb_obliqr, orb_lambm0, orb_mvelpp, rc)
    !?   if (ChkErr(rc,__LINE__,u_FILE_u)) return

    !?   write(stdout,*) ' '
    !?   call document ('DataInitialize', 'orb_eccen   ',  orb_eccen)
    !?   call document ('DataInitialize', 'orb_mvelpp  ',  orb_mvelpp)
    !?   call document ('DataInitialize', 'orb_lambm0  ',  orb_lambm0)
    !?   call document ('DataInitialize', 'orb_obliqr  ',  orb_obliqr)
    !?endif

    !-----------------------------------------------------------------------
    ! check whether all Fields in the exportState are "Updated"
    !-----------------------------------------------------------------------

    if (NUOPC_IsUpdated(exportState)) then
      call NUOPC_CompAttributeSet(gcomp, name="InitializeDataComplete", value="true", rc=rc)

      call ESMF_LogWrite("MPASo - Initialize-Data-Dependency SATISFIED!!!", ESMF_LOGMSG_INFO)
      if (ESMF_LogFoundError(rcToCheck=rc, msg=ESMF_LOGERR_PASSTHRU, &
        line=__LINE__, &
        file=__FILE__)) &
        return  ! bail out
    end if

    !?!-----------------------------------------------------------------------
    !?! Now document all time flags, last step of pop initialization
    !?!-----------------------------------------------------------------------

    !?call document_time_flags

    !?!-----------------------------------------------------------------------
    !?! Output delimiter to log file
    !?!-----------------------------------------------------------------------

    !?if (mastertask) then
    !?   write(stdout,blank_fmt)
    !?   write(stdout,'(" End of initialization")')
    !?   write(stdout,blank_fmt)
    !?   write(stdout,ndelim_fmt)
    !?   call POP_IOUnitsFlush(POP_stdout)
    !?   call POP_IOUnitsFlush(stdout)
    !?endif

    !?!----------------------------------------------------------------------------
    !?! Reset shr logging to original values
    !?!----------------------------------------------------------------------------

    !?call shr_file_setLogUnit (shrlogunit)

  end subroutine DataInitialize

  !===============================================================================

  subroutine ModelAdvance(gcomp, rc)

    !-----------------------------------------------------------------------
    ! Run POP for a coupling interval
    !-----------------------------------------------------------------------

    ! input/output variables
    type(ESMF_GridComp)  :: gcomp
    integer, intent(out) :: rc

    !  local variables
    type(ESMF_State)             :: importState
    type(ESMF_State)             :: exportState
    type(ESMF_Clock)             :: clock
    type(ESMF_Alarm)             :: alarm
    type(ESMF_Time)              :: currTime
    type(ESMF_Time)              :: nextTime
    character(ESMF_MAXSTR)       :: cvalue
    integer                      :: errorCode  ! error flag
    integer                      :: ymd        ! POP2 current date (YYYYMMDD)
    integer                      :: tod        ! POP2 current time of day (sec)
    integer                      :: ymd_sync   ! Sync clock current date (YYYYMMDD)
    integer                      :: tod_sync   ! Sync clcok current time of day (sec)
    integer                      :: lbnum
    integer                      :: ierr
    integer                      :: yr_sync
    integer                      :: mon_sync
    integer                      :: day_sync
    integer                      :: shrlogunit ! old values
    character(CL)          :: message
    logical                      :: first_time = .true.
    character(len=*), parameter  :: subname = "ocn_comp_nuopc: (ModelAdvance)"
    !-----------------------------------------------------------------------

    rc = ESMF_SUCCESS
    errorCode = ESMF_Success

    !?!-----------------------------------------------------------------------
    !?! skip first coupling interval for an initial run
    !?!-----------------------------------------------------------------------

    !?! NOTE: pop starts one coupling interval ahead of the rest of the system
    !?! so to have it be in sync with the rest of the system, simply skip the first
    !?! coupling interval for a initial run only
    !?if (first_time) then
    !?   first_time = .false.
    !?   if (runtype == 'initial') then
    !?      if (mastertask) then
    !?         write(stdout,*)'Returning at first coupling interval'
    !?      end if
    !?      RETURN
    !?   end if
    !?end if

#if (defined _MEMTRACE)
    if(iam == 0 ) then
       lbnum=1
       call memmon_dump_fort('memmon.out',subname//':start::',lbnum)
    endif
#endif

    !?!--------------------------------------------------------------------
    !?! check that pop internal clock is in sync with ESMF clock
    !?!--------------------------------------------------------------------

    !?! NOTE: that in nuopc - the ESMF clock is updated at the end of the timestep
    !?! whereas in cpl7 it was updated at the beginning - so need to have the check
    !?! at the beginning of the time loop BEFORE pop updates its time step

    !?! pop clock
    !?ymd = iyear*10000 + imonth*100 + iday
    !?tod = ihour*seconds_in_hour + iminute*seconds_in_minute + isecond

    !?! model clock
    !?call NUOPC_ModelGet(gcomp, modelClock=clock, rc=rc)
    !?if (ChkErr(rc,__LINE__,u_FILE_u)) return
    !?call ESMF_ClockGet( clock, currTime=currTime, rc=rc)
    !?if (ChkErr(rc,__LINE__,u_FILE_u)) return
    !?call ESMF_TimeGet( currTime, yy=yr_sync, mm=mon_sync, dd=day_sync, s=tod_sync, rc=rc )
    !?if (ChkErr(rc,__LINE__,u_FILE_u)) return
    !?call shr_cal_ymd2date(yr_sync, mon_sync, day_sync, ymd_sync)

    !?! check
    !?if ( (ymd /= ymd_sync) .or. (tod /= tod_sync) ) then
    !?   write(stdout,*)' pop2 ymd=',ymd     ,'  pop2 tod= ',tod
    !?   write(stdout,*)' sync ymd=',ymd_sync,'  sync tod= ',tod_sync
    !?   write(stdout,*)' Internal pop2 clock not in sync with Sync Clock'
    !?   call ESMF_LogWrite(subname//" Internal POP clock not in sync with ESMF model clock", ESMF_LOGMSG_INFO)
    !?   !call shr_sys_abort(subName// ":: Internal POP clock not in sync with ESMF model Clock")
    !?end if
    !?!-----------------------------------------------------------------------
    !?!  start up the main timer
    !?!-----------------------------------------------------------------------

    !?call timer_start(timer_total)

    !?!-----------------------------------------------------------------------
    !?! reset shr logging to my log file
    !?!----------------------------------------------------------------------------

    !?call shr_file_getLogUnit (shrlogunit)
    !?call shr_file_setLogUnit (stdout)

    !?if (ldiag_cpl) then
    !?   call register_string ('info_debug_ge2')
    !?endif

    !--------------------------------
    ! Query the Component for its clock, importState and exportState
    !--------------------------------

    call NUOPC_ModelGet(gcomp, modelClock=clock, importState=importState, exportState=exportState, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    !?!----------------------------------------------------------------------------
    !?! restart flag (rstwr) will assume only an eod restart for now
    !?!----------------------------------------------------------------------------

    ! Note this logic triggers off of the component clock rather than the internal pop time
    ! The component clock does not get advanced until the end of the loop - not at the beginning

    call ESMF_ClockGetAlarm(clock, alarmname='alarm_restart', alarm=alarm, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    if (ESMF_AlarmIsRinging(alarm, rc=rc)) then
       if (ChkErr(rc,__LINE__,u_FILE_u)) return
       call ESMF_AlarmRingerOff( alarm, rc=rc )
       if (ChkErr(rc,__LINE__,u_FILE_u)) return

       !?call override_time_flag(cpl_write_restart, value=.true.)
       !?call ccsm_char_date_and_time ! set time_management module vars cyear, cmonth, ..
       !?write(message,'(6a)') 'driver requests restart file at eod  ', cyear,'/',cmonth,'/',cday
       !?call document ('ModelAdvance:', message)
    endif

    !-----------------------------------------------------------------------
    ! advance the model in time over coupling interval
    ! write restart dumps and archiving
    !-----------------------------------------------------------------------

    ! Note that all ocean time flags are evaluated each timestep in time_manager
    !?! tlast_coupled is set to zero at the end of ocn_export

print*,'model_advance'
    advance: do

print*,'model_advance_loop'
       ! -----
       ! obtain import state data
       ! -----
      !? if (check_time_flag(cpl_ts) .or. nsteps_run == 0) then

          call ocn_import(importState, flds_scalar_name, domain_ptr,  &
                          errorCode, rc)
      !?    if (ChkErr(rc,__LINE__,u_FILE_u)) return

      !?    if (errorCode /= POP_Success) then
      !?       call POP_ErrorPrint(errorCode)
      !?       call exit_POP(sigAbort, 'ERROR in step')
      !?    endif

      !?    ! update orbital parameters

      !?    call pop_orbital_update(clock, stdout, mastertask, orb_eccen, orb_obliqr, orb_lambm0, orb_mvelpp, rc)
      !?    if (ChkErr(rc,__LINE__,u_FILE_u)) return

      !?    call pop_set_coupled_forcing
      !? end if

       ! -----
       ! advance mpaso
       ! -----

       iErr = domain_ptr % core % core_run(domain_ptr)
       if ( iErr /= 0 ) then
         call mpas_log_write('Core run failed for core '//trim(domain_ptr % core % coreName), messageType=MPAS_LOG_CRIT)
       end if

       !?if (errorCode /= POP_Success) then
       !?   call POP_ErrorPrint(errorCode)
       !?   call exit_POP(sigAbort, 'ERROR in step')
       !?endif

       !?if (check_KE(100.0_r8)) then
       !?   !*** exit if energy is blowing
       !?   call output_driver
       !?   call exit_POP(sigAbort,'ERROR: k.e. > 100 ')
       !?endif
       !?call output_driver()

       ! -----
       ! create export state
       ! -----

       !?if (check_time_flag(cpl_ts)) then

          call ocn_export(exportState, flds_scalar_name, domain_ptr,    &
                          errorCode, rc)
          if (ChkErr(rc,__LINE__,u_FILE_u)) return

       !?   if (errorCode /= POP_Success) then
       !?      call POP_ErrorPrint(errorCode)
       !?      call exit_POP(sigAbort, 'ERROR in ocn_export')
       !?   endif

       !?   if ( lsend_precip_fact ) then
       !?      call State_SetScalar(precip_fact, flds_scalar_index_precip_factor, exportState, &
       !?           flds_scalar_name, flds_scalar_num, rc)
       !?      if (ChkErr(rc,__LINE__,u_FILE_u)) return
       !?   else
       !?      ! Just send back 1.
       !?      call State_SetScalar(1._r8, flds_scalar_index_precip_factor, exportState, &
       !?           flds_scalar_name, flds_scalar_num, rc)
       !?      if (ChkErr(rc,__LINE__,u_FILE_u)) return
       !?   end if

          exit advance
       !?end if

    enddo advance

    !?!----------------------------------------------------------------------------
    !?! Reset shr logging to original values
    !?!----------------------------------------------------------------------------

    !?call shr_file_setLogUnit (shrlogunit)

    !?call timer_stop(timer_total)

#if (defined _MEMTRACE)
    if(iam == 0) then
       lbnum=1
       call memmon_dump_fort('memmon.out',subname//':end::',lbnum)
       call memmon_reset_addr()
    endif
#endif

  end subroutine ModelAdvance

  !===============================================================================

  subroutine ModelSetRunClock(gcomp, rc)

    ! input/output variables
    type(ESMF_GridComp)  :: gcomp
    integer, intent(out) :: rc

    ! local variables
    type(ESMF_Clock)         :: mclock, dclock
    type(ESMF_Time)          :: mcurrtime, dcurrtime
    type(ESMF_Time)          :: mstoptime
    type(ESMF_TimeInterval)  :: mtimestep, dtimestep
    character(len=256)       :: cvalue
    character(len=256)       :: restart_option ! Restart option units
    integer                  :: restart_n      ! Number until restart interval
    integer                  :: restart_ymd    ! Restart date (YYYYMMDD)
    type(ESMF_ALARM)         :: restart_alarm
    character(len=256)       :: stop_option    ! Stop option units
    integer                  :: stop_n         ! Number until stop interval
    integer                  :: stop_ymd       ! Stop date (YYYYMMDD)
    type(ESMF_ALARM)         :: stop_alarm
    character(len=128)       :: name
    integer                  :: alarmcount
    character(len=*),parameter :: subname='ocn_comp_nuopc:(ModelSetRunClock)'
    !--------------------------------

    rc = ESMF_SUCCESS
    if (dbug > 5) call ESMF_LogWrite(subname//' called', ESMF_LOGMSG_INFO)

    ! query the Component for its clocks
    call NUOPC_ModelGet(gcomp, driverClock=dclock, modelClock=mclock, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    call ESMF_ClockGet(dclock, currTime=dcurrtime, timeStep=dtimestep, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    call ESMF_ClockGet(mclock, currTime=mcurrtime, timeStep=mtimestep, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    !--------------------------------
    ! force model clock currtime and timestep to match driver and set stoptime
    !--------------------------------

    mstoptime = mcurrtime + dtimestep
    call ESMF_ClockSet(mclock, currTime=dcurrtime, timeStep=dtimestep, stopTime=mstoptime, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    !--------------------------------
    ! set restart and stop alarms
    !--------------------------------

    call ESMF_ClockGetAlarmList(mclock, alarmlistflag=ESMF_ALARMLIST_ALL, alarmCount=alarmCount, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    if (alarmCount == 0) then

       call ESMF_GridCompGet(gcomp, name=name, rc=rc)
       if (ChkErr(rc,__LINE__,u_FILE_u)) return
       call ESMF_LogWrite(subname//'setting alarms for' // trim(name), ESMF_LOGMSG_INFO)

       !----------------
       ! Restart alarm
       !----------------
       call NUOPC_CompAttributeGet(gcomp, name="restart_option", value=restart_option, rc=rc)
       if (ChkErr(rc,__LINE__,u_FILE_u)) return

       call NUOPC_CompAttributeGet(gcomp, name="restart_n", value=cvalue, rc=rc)
       if (ChkErr(rc,__LINE__,u_FILE_u)) return
       read(cvalue,*) restart_n

       call NUOPC_CompAttributeGet(gcomp, name="restart_ymd", value=cvalue, rc=rc)
       if (ChkErr(rc,__LINE__,u_FILE_u)) return
       read(cvalue,*) restart_ymd

       call alarmInit(mclock, restart_alarm, restart_option, &
            opt_n   = restart_n,           &
            opt_ymd = restart_ymd,         &
            RefTime = mcurrTime,           &
            alarmname = 'alarm_restart', rc=rc)
       if (ChkErr(rc,__LINE__,u_FILE_u)) return

       call ESMF_AlarmSet(restart_alarm, clock=mclock, rc=rc)
       if (ChkErr(rc,__LINE__,u_FILE_u)) return

       !----------------
       ! Stop alarm
       !----------------
       call NUOPC_CompAttributeGet(gcomp, name="stop_option", value=stop_option, rc=rc)
       if (ChkErr(rc,__LINE__,u_FILE_u)) return

       call NUOPC_CompAttributeGet(gcomp, name="stop_n", value=cvalue, rc=rc)
       if (ChkErr(rc,__LINE__,u_FILE_u)) return
       read(cvalue,*) stop_n

       call NUOPC_CompAttributeGet(gcomp, name="stop_ymd", value=cvalue, rc=rc)
       if (ChkErr(rc,__LINE__,u_FILE_u)) return
       read(cvalue,*) stop_ymd

       call alarmInit(mclock, stop_alarm, stop_option, &
            opt_n   = stop_n,           &
            opt_ymd = stop_ymd,         &
            RefTime = mcurrTime,           &
            alarmname = 'alarm_stop', rc=rc)
       if (ChkErr(rc,__LINE__,u_FILE_u)) return

       call ESMF_AlarmSet(stop_alarm, clock=mclock, rc=rc)
       if (ChkErr(rc,__LINE__,u_FILE_u)) return

    end if

    !--------------------------------
    ! Advance model clock to trigger alarms then reset model clock back to currtime
    !--------------------------------

    call ESMF_ClockAdvance(mclock,rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    call ESMF_ClockSet(mclock, currTime=dcurrtime, timeStep=dtimestep, stopTime=mstoptime, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    if (dbug > 5) call ESMF_LogWrite(subname//' done', ESMF_LOGMSG_INFO)

  end subroutine ModelSetRunClock

  !===============================================================================

  subroutine ModelCheckImport(model, rc)

    ! input/output variables
    type(ESMF_GridComp)  :: model
    integer, intent(out) :: rc

    ! local variables
    type(ESMF_Clock)  :: clock
    type(ESMF_Time)   :: currTime
    integer           :: yy  ! current date (YYYYMMDD)
    integer           :: mon ! current month
    integer           :: day ! current day
    integer           :: tod ! current time of day (sec)
    !-----------------------------------------------------------------------

    rc = ESMF_SUCCESS

    ! query the Component for its clock, importState and exportState
    call NUOPC_ModelGet(model, modelClock=clock, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    call ESMF_ClockGet( clock, currTime=currTime, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    call ESMF_TimeGet(currTime, yy=yy, mm=mon, dd=day, s=tod, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    !?if (mastertask) then
    !?   write(stdout,*)' CheckImport pop2 year = ',yy
    !?   write(stdout,*)' CheckImport pop2 mon  = ',mon
    !?   write(stdout,*)' CheckImport pop2 day  = ',day
    !?   write(stdout,*)' CheckImport pop2 tod  = ',tod
    !?end if

  end subroutine ModelCheckImport


  !===============================================================================

  subroutine ModelFinalize(gcomp, rc)

    !--------------------------------
    ! MPASO finalization that shuts down MPASO gracefully (we hope).
    ! Exits the message environment and checks for successful execution.
    ! --------------------------------


    ! input/output variables
    type(ESMF_GridComp)  :: gcomp
    integer, intent(out) :: rc

    ! local variables
    integer  :: ierr              ! error code
    character(len=*),parameter :: subname='ocn_comp_nuopc:(ModelFinalize)'
    !--------------------------------

    rc = ESMF_SUCCESS
    if (dbug > 5) call ESMF_LogWrite(subname//' called', ESMF_LOGMSG_INFO)

      iErr = domain_ptr % core % core_finalize(domain_ptr)
      if ( iErr /= 0 ) then
         call mpas_log_write('Core finalize failed for core '//trim(domain_ptr % core % coreName), messageType=MPAS_LOG_CRIT)
      end if

      call mpas_timer_stop('total time')
      call mpas_timer_write_header()
      call mpas_timer_write()
      call mpas_timer_finalize(domain_ptr)

      !
      ! Finalize infrastructure
      !
      call MPAS_stream_mgr_finalize(domain_ptr % streamManager)

      ! Print out log stats and close log file
      !   (Do this after timer stats are printed and stream mgr finalized,
      !    but before framework is finalized because domain is destroyed there.)
      call mpas_log_finalize(iErr)
      if ( iErr /= 0 ) then
         call mpas_dmpar_global_abort('ERROR: Log finalize failed for core ' // trim(domain_ptr % core % coreName))
      end if

      call mpas_framework_finalize(domain_ptr % dminfo, domain_ptr)

      deallocate(corelist % domainlist)
      deallocate(corelist)

    !?!  exit the communication environment
    !?call exit_message_environment(ErrorCode)

    if (dbug > 5) call ESMF_LogWrite(subname//' done', ESMF_LOGMSG_INFO)

  end subroutine ModelFinalize

   subroutine add_stream_attributes(domain)

      use mpas_stream_manager, only : MPAS_stream_mgr_add_att

      implicit none

      type (domain_type), intent(inout) :: domain

      type (MPAS_Pool_iterator_type) :: itr
      integer, pointer :: intAtt
      logical, pointer :: logAtt
      character (len=StrKIND), pointer :: charAtt
      real (kind=RKIND), pointer :: realAtt
      character (len=StrKIND) :: histAtt

      integer :: local_ierr

      if (domain % dminfo % nProcs < 10) then
          write(histAtt, '(A,I1,A,A,A)') 'mpirun -n ', domain % dminfo % nProcs, ' ./', trim(domain % core % coreName), '_model'
      else if (domain % dminfo % nProcs < 100) then
          write(histAtt, '(A,I2,A,A,A)') 'mpirun -n ', domain % dminfo % nProcs, ' ./', trim(domain % core % coreName), '_model'
      else if (domain % dminfo % nProcs < 1000) then
          write(histAtt, '(A,I3,A,A,A)') 'mpirun -n ', domain % dminfo % nProcs, ' ./', trim(domain % core % coreName), '_model'
      else if (domain % dminfo % nProcs < 10000) then
          write(histAtt, '(A,I4,A,A,A)') 'mpirun -n ', domain % dminfo % nProcs, ' ./', trim(domain % core % coreName), '_model'
      else if (domain % dminfo % nProcs < 100000) then
          write(histAtt, '(A,I5,A,A,A)') 'mpirun -n ', domain % dminfo % nProcs, ' ./', trim(domain % core % coreName), '_model'
      else
          write(histAtt, '(A,I6,A,A,A)') 'mpirun -n ', domain % dminfo % nProcs, ' ./', trim(domain % core % coreName), '_model'
      end if
     
      call MPAS_stream_mgr_add_att(domain % streamManager, 'model_name', domain % core % modelName)
      call MPAS_stream_mgr_add_att(domain % streamManager, 'core_name', domain % core % coreName)
      call MPAS_stream_mgr_add_att(domain % streamManager, 'source', domain % core % source)
      call MPAS_stream_mgr_add_att(domain % streamManager, 'Conventions', domain % core % Conventions)
      call MPAS_stream_mgr_add_att(domain % streamManager, 'git_version', domain % core % git_version)

      call MPAS_stream_mgr_add_att(domain % streamManager, 'on_a_sphere', domain % on_a_sphere)
      call MPAS_stream_mgr_add_att(domain % streamManager, 'sphere_radius', domain % sphere_radius)
      call MPAS_stream_mgr_add_att(domain % streamManager, 'is_periodic', domain % is_periodic)
      call MPAS_stream_mgr_add_att(domain % streamManager, 'x_period', domain % x_period)
      call MPAS_stream_mgr_add_att(domain % streamManager, 'y_period', domain % y_period)
      ! DWJ 10/01/2014: Eventually add the real history attribute, for now (due to length restrictions)
      ! add a shortened version.
!     call MPAS_stream_mgr_add_att(domain % streamManager, 'history', domain % history)
      call MPAS_stream_mgr_add_att(domain % streamManager, 'history', histAtt)
      call MPAS_stream_mgr_add_att(domain % streamManager, 'parent_id', domain %  parent_id)
      call MPAS_stream_mgr_add_att(domain % streamManager, 'mesh_spec', domain % mesh_spec)

      call mpas_pool_begin_iteration(domain % configs)

      do while (mpas_pool_get_next_member(domain % configs, itr))

         if ( itr % memberType == MPAS_POOL_CONFIG) then

            if ( itr % dataType == MPAS_POOL_REAL ) then
               call mpas_pool_get_config(domain % configs, itr % memberName, realAtt)
               call MPAS_stream_mgr_add_att(domain % streamManager, itr % memberName, realAtt, ierr=local_ierr)
            else if ( itr % dataType == MPAS_POOL_INTEGER ) then
               call mpas_pool_get_config(domain % configs, itr % memberName, intAtt)
               call MPAS_stream_mgr_add_att(domain % streamManager, itr % memberName, intAtt, ierr=local_ierr)
            else if ( itr % dataType == MPAS_POOL_CHARACTER ) then
               call mpas_pool_get_config(domain % configs, itr % memberName, charAtt)
               call MPAS_stream_mgr_add_att(domain % streamManager, itr % memberName, charAtt, ierr=local_ierr)
            else if ( itr % dataType == MPAS_POOL_LOGICAL ) then
               call mpas_pool_get_config(domain % configs, itr % memberName, logAtt)
               if (logAtt) then
                  call MPAS_stream_mgr_add_att(domain % streamManager, itr % memberName, 'YES', ierr=local_ierr)
               else
                  call MPAS_stream_mgr_add_att(domain % streamManager, itr % memberName, 'NO', ierr=local_ierr)
               end if
            end if

          end if
      end do

   end subroutine add_stream_attributes


end module ocn_comp_nuopc
