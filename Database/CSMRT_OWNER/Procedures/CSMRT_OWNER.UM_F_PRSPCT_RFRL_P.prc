DROP PROCEDURE CSMRT_OWNER.UM_F_PRSPCT_RFRL_P
/

--
-- UM_F_PRSPCT_RFRL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_F_PRSPCT_RFRL_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table              -- UM_F_PRSPCT_RFRL
--UM_F_PRSPCT_RFRL         -- PS_D_INSTITUTION ;PS_D_ACAD_CAR;PS_D_PERSON;UM_D_PRSPCT_CAR;PS_D_RECRT_CNTR;UM_D_RFRL_DTL
--V01 11/28/2018           -- srikanth ,pabbu converted to proc from sql

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_PRSPCT_RFRL';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_PRSPCT_RFRL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_PRSPCT_RFRL';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_PRSPCT_RFRL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_PRSPCT_RFRL');

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_PRSPCT_RFRL disable constraint PK_UM_F_PRSPCT_RFRL';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_PRSPCT_RFRL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_PRSPCT_RFRL';
insert /*+ append enable_parallel_dml parallel(8) */ into UM_F_PRSPCT_RFRL
  with PC as (
select /*+ inline parallel(16) */
       INSTITUTION_CD, ACAD_CAR_CD, EMPLID, SRC_SYS_ID, min(PRSPCT_CAR_SID) PRSPCT_CAR_SID
  from CSMRT_OWNER.UM_D_PRSPCT_CAR
 where DATA_ORIGIN <> 'D'
 group by INSTITUTION_CD, ACAD_CAR_CD, EMPLID, SRC_SYS_ID),
       R as (
select EMPLID, ACAD_CAREER, INSTITUTION, UM_REFRL_GRP, UM_REFRL_DTL, UM_REFRL_DATE, ADMIT_TERM, UM_ADM_REC_NBR, SRC_SYS_ID,
       ADM_RECR_CTR, DATA_ORIGIN,
       row_number() over (partition by EMPLID, ACAD_CAREER, INSTITUTION, UM_REFRL_GRP, UM_REFRL_DTL, SRC_SYS_ID
                              order by UM_REFRL_DATE desc) R_ORDER
  from CSSTG_OWNER.PS_UM_PRSPCT_REFL
 where DATA_ORIGIN <> 'D')
select /*+ parallel(16) */
       R.INSTITUTION as INSTITUTION_CD,
	   R.ACAD_CAREER as ACAD_CAR_CD,
	   R.ADMIT_TERM,
	   R.EMPLID,
       R.UM_REFRL_GRP as RFRL_GRP,
       R.UM_REFRL_DTL as RFRL_DTL,
       R.UM_REFRL_DATE as RFRL_DT,
       R.UM_ADM_REC_NBR,
       R.SRC_SYS_ID,
	   nvl(I.INSTITUTION_SID, 2147483646) INSTITUTION_SID,
	   nvl(C.ACAD_CAR_SID, 2147483646) ACAD_CAR_SID,
       nvl(T.TERM_SID,2147483646) ADMIT_TERM_SID,
	   nvl(P.PERSON_SID, 2147483646) PERSON_SID,
	   nvl(PC.PRSPCT_CAR_SID,2147483646) PRSPCT_CAR_SID,
	   nvl(RC.RECRT_CNTR_SID,2147483646) RECRT_CNTR_SID,
	   nvl(RD.RFRL_DTL_SID,2147483646) RFRL_DTL_SID,
       R.ADM_RECR_CTR,
	   'N' LOAD_ERROR,
       'S' DATA_ORIGIN,
       SYSDATE CREATED_EW_DTTM,
       SYSDATE LASTUPD_EW_DTTM,
       1234 BATCH_SID
  from R
  left outer join CSMRT_OWNER.PS_D_INSTITUTION I
    on R.INSTITUTION = I.INSTITUTION_CD
   and R.SRC_SYS_ID = I.SRC_SYS_ID
   and I.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_ACAD_CAR C
    on R.ACAD_CAREER = C.ACAD_CAR_CD
   and R.INSTITUTION = C.INSTITUTION_CD
   and R.SRC_SYS_ID = C.SRC_SYS_ID
   and C.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_TERM T
    on R.INSTITUTION = T.INSTITUTION_CD
   and R.ACAD_CAREER = T.ACAD_CAR_CD
   and R.ADMIT_TERM = T.TERM_CD
   and R.SRC_SYS_ID = T.SRC_SYS_ID
   and T.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_PERSON P
    on R.EMPLID = P.PERSON_ID
   and R.SRC_SYS_ID = P.SRC_SYS_ID
   and P.DATA_ORIGIN <> 'D'
  join PC
    on R.INSTITUTION = PC.INSTITUTION_CD
   and R.ACAD_CAREER = PC.ACAD_CAR_CD
   and R.EMPLID = PC.EMPLID
   and R.SRC_SYS_ID = PC.SRC_SYS_ID
  left outer join CSMRT_OWNER.PS_D_RECRT_CNTR RC
    on R.INSTITUTION = RC.INSTITUTION_CD
   and R.ADM_RECR_CTR = RC.RECRT_CNTR_ID
   and R.SRC_SYS_ID = RC.SRC_SYS_ID
   and RC.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.UM_D_RFRL_DTL RD
    on R.INSTITUTION = RD.INSTITUTION_CD
   and R.UM_REFRL_GRP = RD.RFRL_GRP
   and R.UM_REFRL_DTL = RD.RFRL_DTL
   and R.SRC_SYS_ID = RD.SRC_SYS_ID
   and RD.DATA_ORIGIN <> 'D'
 where R.R_ORDER = 1
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_PRSPCT_RFRL rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_PRSPCT_RFRL',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_PRSPCT_RFRL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_PRSPCT_RFRL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_PRSPCT_RFRL enable constraint PK_UM_F_PRSPCT_RFRL';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_PRSPCT_RFRL');

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

END UM_F_PRSPCT_RFRL_P;
/
