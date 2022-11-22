DROP PROCEDURE CSMRT_OWNER.UM_R_PERSON_RSDNCY_P
/

--
-- UM_R_PERSON_RSDNCY_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_R_PERSON_RSDNCY_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table                -- UM_R_PERSON_RSDNCY
--V01 12/12/2018             -- srikanth ,pabbu converted to proc from sql scripts

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_R_PERSON_RSDNCY';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_R_PERSON_RSDNCY';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_R_PERSON_RSDNCY';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_R_PERSON_RSDNCY';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_R_PERSON_RSDNCY');

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_R_PERSON_RSDNCY disable constraint PK_UM_R_PERSON_RSDNCY';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_R_PERSON_RSDNCY';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_R_PERSON_RSDNCY';
insert /*+ append parallel(8) enable_parallel_dml */ into CSMRT_OWNER.UM_R_PERSON_RSDNCY
with X as (
select /*+ inline parallel(8) */
       FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID,
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN,
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1900') else EFFDT end) desc) X_ORDER
  from CSSTG_OWNER.PSXLATITEM
 where DATA_ORIGIN <> 'D'),
       T as (
select /*+ INLINE PARALLEL(8) */ distinct
       INSTITUTION_CD, ACAD_CAR_CD, TERM_CD, PERSON_ID, INSTITUTION_SID, ACAD_CAR_SID, TERM_SID, PERSON_SID, SRC_SYS_ID
  from UM_F_ACAD_PROG
 where TERM_SID <> 2147483646
   and PERSON_SID <> 2147483646
 union
select /*+ INLINE PARALLEL(8) */ distinct
       INSTITUTION_CD, ACAD_CAR_CD, TERM_CD, PERSON_ID, INSTITUTION_SID, ACAD_CAR_SID, TERM_SID, PERSON_SID, SRC_SYS_ID
  from UM_F_FA_TERM
 where TERM_SID <> 2147483646
   and PERSON_SID <> 2147483646
 union
select /*+ INLINE PARALLEL(8) */ distinct
       F.INSTITUTION_CD, F.ACAD_CAR_CD, T.TERM_CD, F.PERSON_ID EMPLID, F.INSTITUTION_SID, F.ACAD_CAR_SID, F.ADMIT_TERM_SID TERM_SID, APPLCNT_SID PERSON_SID, F.SRC_SYS_ID
  from PS_F_ADM_APPL_STAT F
  join CSMRT_OWNER.PS_D_TERM T
    on F.ADMIT_TERM_SID = T.TERM_SID
 union
select /*+ INLINE PARALLEL(8) */ distinct
       C.INSTITUTION_CD, C.ACAD_CAR_CD, T.TERM_CD, P.PERSON_ID, C.INSTITUTION_SID, C.ACAD_CAR_SID, C.ADMIT_TERM_SID, C.PERSON_SID, C.SRC_SYS_ID
  from UM_D_PRSPCT_CAR C, PS_D_PERSON P, PS_D_TERM T
 where C.PERSON_SID = P.PERSON_SID
   and C.ADMIT_TERM_SID = T.TERM_SID
   and C.ADMIT_TERM_SID <> 2147483646
   and C.PERSON_SID <> 2147483646
),
RES as (
select /*+ INLINE PARALLEL(8) */
T.INSTITUTION_CD, T.ACAD_CAR_CD, T.TERM_CD EFF_TERM_CD, T.PERSON_ID, T.SRC_SYS_ID,
T.INSTITUTION_SID, T.ACAD_CAR_SID, T.TERM_SID EFF_TERM_SID, T.PERSON_SID,
R.RSDNCY_SID, R.RSDNCY_ID, R.RSDNCY_LD,
R.ADM_RSDNCY_SID, R.ADM_RSDNCY_ID, R.ADM_RSDNCY_LD,
R.FA_FED_RSDNCY_SID, R.FA_FED_RSDNCY_ID, R.FA_FED_RSDNCY_LD,
R.FA_ST_RSDNCY_SID, R.FA_ST_RSDNCY_ID, R.FA_ST_RSDNCY_LD,
R.TUITION_RSDNCY_SID, R.TUITION_RSDNCY_ID, R.TUITION_RSDNCY_LD,
R.RSDNCY_TERM_SID, R.RSDNCY_TERM_CD,
R.ADM_EXCPT_SID, R.ADM_RSDNCY_EXCPTN, R.ADM_RSDNCY_EXCPTN_LD,
R.FA_FED_EXCPT_SID, R.FA_FED_RSDNCY_EXCPTN, R.FA_FED_RSDNCY_EXCPTN_LD,
R.FA_ST_EXCPT_SID, R.FA_ST_RSDNCY_EXCPTN, R.FA_ST_RSDNCY_EXCPTN_LD,
R.TUITION_EXCPT_SID, R.TUITION_RSDNCY_EXCPTN, R.TUITION_RSDNCY_EXCPTN_LD,
R.RSDNCY_DT, R.APPEAL_EFFDT, R.APPEAL_STATUS, R.APPEAL_COMMENTS,
ROW_NUMBER() OVER (PARTITION BY T.PERSON_ID, T.ACAD_CAR_CD, T.INSTITUTION_CD, T.TERM_CD
                       ORDER BY R.EFF_TERM_CD desc) RESIDENCY_ORDER
from T
left outer join PS_R_PERSON_RSDNCY R
   on T.PERSON_ID = R.PERSON_ID
  and T.ACAD_CAR_CD = R.ACAD_CAR_CD
  and T.INSTITUTION_CD = R.INSTITUTION_CD
  and T.TERM_CD >= R.EFF_TERM_CD
)
select /*+ INLINE PARALLEL(8) */
INSTITUTION_CD, ACAD_CAR_CD, EFF_TERM_CD, PERSON_ID, RES.SRC_SYS_ID,
nvl(INSTITUTION_SID,2147483646) INSTITUTION_SID,
nvl(ACAD_CAR_SID,2147483646) ACAD_CAR_SID,
nvl(EFF_TERM_SID,2147483646) EFF_TERM_SID,
nvl(PERSON_SID,2147483646) PERSON_SID,
nvl(RSDNCY_SID,2147483646) RSDNCY_SID,
nvl(RSDNCY_ID,'-') RSDNCY_ID,
nvl(RSDNCY_LD,'-') RSDNCY_LD,
nvl(ADM_RSDNCY_SID,2147483646) ADM_RSDNCY_SID,
nvl(ADM_RSDNCY_ID,'-') ADM_RSDNCY_ID,
nvl(ADM_RSDNCY_LD,'-') ADM_RSDNCY_LD,
nvl(FA_FED_RSDNCY_SID,2147483646) FA_FED_RSDNCY_SID,
nvl(FA_FED_RSDNCY_ID,'-') FA_FED_RSDNCY_ID,
nvl(FA_FED_RSDNCY_LD,'-') FA_FED_RSDNCY_LD,
nvl(FA_ST_RSDNCY_SID,2147483646) FA_ST_RSDNCY_SID,
nvl(FA_ST_RSDNCY_ID,'-') FA_ST_RSDNCY_ID,
nvl(FA_ST_RSDNCY_LD,'-') FA_ST_RSDNCY_LD,
nvl(TUITION_RSDNCY_SID,2147483646) TUITION_RSDNCY_SID,
nvl(TUITION_RSDNCY_ID,'-') TUITION_RSDNCY_ID,
nvl(TUITION_RSDNCY_LD,'-') TUITION_RSDNCY_LD,
nvl(RSDNCY_TERM_SID,2147483646) RSDNCY_TERM_SID,
nvl(RSDNCY_TERM_CD,'-') RSDNCY_TERM_CD,
nvl(ADM_EXCPT_SID,2147483646) ADM_EXCPT_SID,
nvl(ADM_RSDNCY_EXCPTN,'-') ADM_RSDNCY_EXCPTN,
nvl(ADM_RSDNCY_EXCPTN_LD,'-') ADM_RSDNCY_EXCPTN_LD,
nvl(FA_FED_EXCPT_SID,2147483646) FA_FED_EXCPT_SID,
nvl(FA_FED_RSDNCY_EXCPTN,'-') FA_FED_RSDNCY_EXCPTN,
nvl(FA_FED_RSDNCY_EXCPTN_LD,'-') FA_FED_RSDNCY_EXCPTN_LD,
nvl(FA_ST_EXCPT_SID,2147483646) FA_ST_EXCPT_SID,
nvl(FA_ST_RSDNCY_EXCPTN,'-') FA_ST_RSDNCY_EXCPTN,
nvl(FA_ST_RSDNCY_EXCPTN_LD,'-') FA_ST_RSDNCY_EXCPTN_LD,
nvl(TUITION_EXCPT_SID,2147483646) TUITION_EXCPT_SID,
nvl(TUITION_RSDNCY_EXCPTN,'-') TUITION_RSDNCY_EXCPTN,
nvl(TUITION_RSDNCY_EXCPTN_LD,'-') TUITION_RSDNCY_EXCPTN_LD,
RSDNCY_DT,
APPEAL_EFFDT,
APPEAL_STATUS,
nvl(X1.XLATSHORTNAME,'-') APPEAL_STATUS_SD,
nvl(X1.XLATLONGNAME,'-') APPEAL_STATUS_LD,
APPEAL_COMMENTS,
'S' DATA_ORIGIN, sysdate CREATED_EW_DTTM, sysdate LASTUPD_EW_DTTM
  from RES
  left outer join X X1
    on RES.APPEAL_STATUS = X1.FIELDVALUE
   and RES.SRC_SYS_ID = X1.SRC_SYS_ID
   and X1.FIELDNAME = 'APPEAL_STATUS'
   and X1.X_ORDER = 1
 where RESIDENCY_ORDER = 1
   and RES.EFF_TERM_SID <> 2147483646		-- Aug 2018
   and RES.PERSON_SID <> 2147483646	    -- Aug 2018
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_R_PERSON_RSDNCY rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_R_PERSON_RSDNCY',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand   := 'insert into CSMRT_OWNER.UM_R_PERSON_RSDNCY';
insert /*+ append enable_parallel_dml parallel(8) */ into CSMRT_OWNER.UM_R_PERSON_RSDNCY
select /*+ INLINE PARALLEL(8) */
INSTITUTION_CD,
'-' ACAD_CAR_CD,
'-' EFF_TERM_CD,
'-' PERSON_ID,
SRC_SYS_ID,
INSTITUTION_SID,
2147483646 ACAD_CAR_SID,
2147483646 EFF_TERM_SID,
2147483646 PERSON_SID,
2147483646 RSDNCY_SID,
'-' RSDNCY_ID,
'-' RSDNCY_LD,
2147483646 ADM_RSDNCY_SID,
'-' ADM_RSDNCY_ID,
'-' ADM_RSDNCY_LD,
2147483646 FA_FED_RSDNCY_SID,
'-' FA_FED_RSDNCY_ID,
'-' FA_FED_RSDNCY_LD,
2147483646 FA_ST_RSDNCY_SID,
'-' FA_ST_RSDNCY_ID,
'-' FA_ST_RSDNCY_LD,
2147483646 TUITION_RSDNCY_SID,
'-' TUITION_RSDNCY_ID,
'-' TUITION_RSDNCY_LD,
2147483646 RSDNCY_TERM_SID,
'-' RSDNCY_TERM_CD,
2147483646 ADM_EXCPT_SID,
'-' ADM_RSDNCY_EXCPTN,
'-' ADM_RSDNCY_EXCPTN_LD,
2147483646 FA_FED_EXCPT_SID,
'-' FA_FED_RSDNCY_EXCPTN,
'-' FA_FED_RSDNCY_EXCPTN_LD,
2147483646 FA_ST_EXCPT_SID,
'-' FA_ST_RSDNCY_EXCPTN,
'-' FA_ST_RSDNCY_EXCPTN_LD,
2147483646 TUITION_EXCPT_SID,
'-' TUITION_RSDNCY_EXCPTN,
'-' TUITION_RSDNCY_EXCPTN_LD,
NULL RSDNCY_DT,
NULL APPEAL_EFFDT,
'-' NULLAPPEAL_STATUS,
'-' APPEAL_STATUS_SD,
'-' APPEAL_STATUS_LD,
NULL APPEAL_COMMENTS,
'S' DATA_ORIGIN,
SYSDATE CREATED_EW_DTTM,
SYSDATE LASTUPD_EW_DTTM
  from CSMRT_OWNER.PS_D_INSTITUTION
 where INSTITUTION_SID <> 2147483646
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_R_PERSON_RSDNCY rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_R_PERSON_RSDNCY',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_R_PERSON_RSDNCY';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_R_PERSON_RSDNCY enable constraint PK_UM_R_PERSON_RSDNCY';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_R_PERSON_RSDNCY');

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

END UM_R_PERSON_RSDNCY_P;
/
