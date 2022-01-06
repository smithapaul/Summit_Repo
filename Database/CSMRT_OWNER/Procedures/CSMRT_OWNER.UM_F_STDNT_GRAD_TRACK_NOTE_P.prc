CREATE OR REPLACE PROCEDURE             "UM_F_STDNT_GRAD_TRACK_NOTE_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads table UM_F_STDNT_GRAD_TRACK_NOTE.
--
 --V01  Case: 80656  11/23/2020  James Doucette
--   
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_STDNT_GRAD_TRACK_NOTE';
        intProcessSid                   Integer;
        dtProcessStart                  Date            := SYSDATE;
        strMessage01                    Varchar2(4000);
        strMessage02                    Varchar2(512);
        strMessage03                    Varchar2(512)   :='';
        strNewLine                      Varchar2(2)     := chr(13) || chr(10);
        strSqlCommand                   Varchar2(32767) :='';
        strSqlDynamic                   Varchar2(32767) :='';
        strClientInfo                   Varchar2(100);
        intRowCount                     Integer;
        intTotalRowCount                Integer         := 0;
        numSqlCode                      Number;
        strSqlErrm                      Varchar2(4000);
        intTries                        Integer;

BEGIN

strSqlCommand := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strProcessName);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_INIT';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT
        (
                i_MartId                => strMartId,
                i_ProcessName           => strProcessName,
                i_ProcessStartTime      => dtProcessStart,
                o_ProcessSid            => intProcessSid
        );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_STDNT_GRAD_TRACK_NOTE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_STDNT_GRAD_TRACK_NOTE');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_STDNT_GRAD_TRACK_NOTE disable constraint PK_UM_F_STDNT_GRAD_TRACK_NOTE';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_STDNT_GRAD_TRACK_NOTE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_STDNT_GRAD_TRACK_NOTE';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_STDNT_GRAD_TRACK_NOTE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_STDNT_GRAD_TRACK_NOTE';				

insert /*+ append */ into CSMRT_OWNER.UM_F_STDNT_GRAD_TRACK_NOTE
  with Q1 as (
select /*+ inline */ INSTITUTION, SSR_GRAD_NOTE, EFFDT, SRC_SYS_ID,
       EFF_STATUS, DESCR,
       row_number() over (partition by INSTITUTION, SSR_GRAD_NOTE, SRC_SYS_ID
                              order by EFFDT desc) Q1_ORDER
  from CSSTG_OWNER.PS_SSR_GRADNOTETBL
 where DATA_ORIGIN <> 'D')
select /*+ inline parallel(16) */ NOTE.EMPLID PERSON_ID, NOTE.INSTITUTION INSTITUTION_CD, NOTE.ACAD_CAREER ACAD_CAR_CD, NOTE.STDNT_CAR_NBR STDNT_CAR_NUM, NOTE.ACAD_PROG ACAD_PROG_CD, NOTE.EXP_GRAD_TERM, NOTE.DEGREE DEG_CD, NOTE.SEQNUM, NOTE.SRC_SYS_ID,
       NOTE.SSR_GRAD_NOTE, nvl(Q1.DESCR,'-') DESCR, NOTE.SCC_ROW_ADD_OPRID, NOTE.SCC_ROW_ADD_DTTM, NOTE.SSR_GRAD_NOTE_LONG,
       'S' DATA_ORIGIN, SYSDATE CREATED_EW_DTTM, SYSDATE LASTUPD_EW_DTTM
  from CSSTG_OWNER.PS_SSR_STDGRD_NOTE NOTE
  left outer join Q1
    on NOTE.INSTITUTION = Q1.INSTITUTION
   and NOTE.SSR_GRAD_NOTE = Q1.SSR_GRAD_NOTE
   and NOTE.SRC_SYS_ID = Q1.SRC_SYS_ID
   and Q1.Q1_ORDER = 1
 where NOTE.DATA_ORIGIN <> 'D'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_STDNT_GRAD_TRACK_NOTE rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_STDNT_GRAD_TRACK_NOTE',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_STDNT_GRAD_TRACK_NOTE',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_STDNT_GRAD_TRACK_NOTE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_STDNT_GRAD_TRACK_NOTE enable constraint PK_UM_F_STDNT_GRAD_TRACK_NOTE';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_STDNT_GRAD_TRACK_NOTE');

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

EXCEPTION
    WHEN OTHERS THEN
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION
                (
                        i_SqlCommand   => strSqlCommand,
                        i_SqlCode      => SQLCODE,
                        i_SqlErrm      => SQLERRM
                );

END UM_F_STDNT_GRAD_TRACK_NOTE_P;
/
