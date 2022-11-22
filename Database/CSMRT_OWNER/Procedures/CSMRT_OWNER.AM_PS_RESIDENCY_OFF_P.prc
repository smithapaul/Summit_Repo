DROP PROCEDURE CSMRT_OWNER.AM_PS_RESIDENCY_OFF_P
/

--
-- AM_PS_RESIDENCY_OFF_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER.AM_PS_RESIDENCY_OFF_P IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_RESIDENCY_OFF from PeopleSoft table PS_RESIDENCY_OFF.
--
-- V01  SMT-xxxx 9/25/2017,    James Doucette
--                             Converted from DataStage
--VXX    07/06/2021,            Kieu ,Srikanth - Added EMPLID or COMMON_ID additional filter logic 
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_RESIDENCY_OFF';
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

strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update START_DT on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Reading',
       START_DT = SYSDATE,
       END_DT = NULL
 where TABLE_NAME = 'PS_RESIDENCY_OFF'
;

strSqlCommand := 'commit';
commit;

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Truncating',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_RESIDENCY_OFF@AMSOURCE S)
 where TABLE_NAME = 'PS_RESIDENCY_OFF'
;

strSqlCommand := 'commit';
commit;

strSqlDynamic   := 'truncate table AMSTG_OWNER.PS_T_RESIDENCY_OFF';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );


strSqlCommand   := 'Loading temp table for AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Loading'
 where TABLE_NAME = 'PS_RESIDENCY_OFF'
;

strSqlCommand := 'commit';
commit;

strSqlCommand := 'insert';
insert /*+ append */  into AMSTG_OWNER.PS_T_RESIDENCY_OFF
select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(EFFECTIVE_TERM),'-') EFFECTIVE_TERM, 
    NVL(RESIDENCY_DT, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )) RESIDENCY_DT,
    nvl(trim(RESIDENCY),'-') RESIDENCY, 
    nvl(trim(ADMISSION_RES),'-') ADMISSION_RES, 
    nvl(trim(FIN_AID_FED_RES),'-') FIN_AID_FED_RES, 
    nvl(trim(FIN_AID_ST_RES),'-') FIN_AID_ST_RES, 
    nvl(trim(TUITION_RES),'-') TUITION_RES, 
    nvl(trim(ADMISSION_EXCPT),'-') ADMISSION_EXCPT, 
    nvl(trim(FIN_AID_FED_EXCPT),'-') FIN_AID_FED_EXCPT, 
    nvl(trim(FIN_AID_ST_EXCPT),'-') FIN_AID_ST_EXCPT, 
    nvl(trim(TUITION_EXCPT),'-') TUITION_EXCPT, 
    nvl(trim(SCC_DISTRICT),'-') SCC_DISTRICT, 
    nvl(trim(CITY),'-') CITY, 
    nvl(trim(COUNTY),'-') COUNTY, 
    nvl(trim(STATE),'-') STATE, 
    nvl(trim(COUNTRY),'-') COUNTRY, 
    nvl(trim(POSTAL),'-') POSTAL, 
    to_char(substr(trim(COMMENTS), 1, 4000)) COMMENTS,
    to_number(ORA_ROWSCN) SRC_SCN  
    from SYSADM.PS_RESIDENCY_OFF@AMSOURCE S
where EMPLID between '00000000' and '99999999'
 and length(EMPLID) = 8 
;

strSqlCommand := 'commit';
commit;

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging'
 where TABLE_NAME = 'PS_RESIDENCY_OFF'
;

strSqlCommand := 'commit';
commit;

strMessage01    := 'Merging data into AMSTG_OWNER.PS_RESIDENCY_OFF';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_RESIDENCY_OFF';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_RESIDENCY_OFF T
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(EFFECTIVE_TERM),'-') EFFECTIVE_TERM, 
    RESIDENCY_DT,
    nvl(trim(RESIDENCY),'-') RESIDENCY, 
    nvl(trim(ADMISSION_RES),'-') ADMISSION_RES, 
    nvl(trim(FIN_AID_FED_RES),'-') FIN_AID_FED_RES, 
    nvl(trim(FIN_AID_ST_RES),'-') FIN_AID_ST_RES, 
    nvl(trim(TUITION_RES),'-') TUITION_RES, 
    nvl(trim(ADMISSION_EXCPT),'-') ADMISSION_EXCPT, 
    nvl(trim(FIN_AID_FED_EXCPT),'-') FIN_AID_FED_EXCPT, 
    nvl(trim(FIN_AID_ST_EXCPT),'-') FIN_AID_ST_EXCPT, 
    nvl(trim(TUITION_EXCPT),'-') TUITION_EXCPT, 
    nvl(trim(SCC_DISTRICT),'-') SCC_DISTRICT, 
    nvl(trim(CITY),'-') CITY, 
    nvl(trim(COUNTY),'-') COUNTY, 
    nvl(trim(STATE),'-') STATE, 
    nvl(trim(COUNTRY),'-') COUNTRY, 
    nvl(trim(POSTAL),'-') POSTAL, 
    to_char(substr(trim(COMMENTS), 1, 4000)) COMMENTS
    from AMSTG_OWNER.PS_T_RESIDENCY_OFF S
where SRC_SCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_RESIDENCY_OFF') 
and EMPLID between '00000000' and '99999999'
   and length(EMPLID) = 8 
) S
 on ( 
    T.EMPLID = S.EMPLID and 
    T.ACAD_CAREER = S.ACAD_CAREER and 
    T.INSTITUTION = S.INSTITUTION and 
    T.EFFECTIVE_TERM = S.EFFECTIVE_TERM and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.RESIDENCY_DT = S.RESIDENCY_DT,
    T.RESIDENCY = S.RESIDENCY,
    T.ADMISSION_RES = S.ADMISSION_RES,
    T.FIN_AID_FED_RES = S.FIN_AID_FED_RES,
    T.FIN_AID_ST_RES = S.FIN_AID_ST_RES,
    T.TUITION_RES = S.TUITION_RES,
    T.ADMISSION_EXCPT = S.ADMISSION_EXCPT,
    T.FIN_AID_FED_EXCPT = S.FIN_AID_FED_EXCPT,
    T.FIN_AID_ST_EXCPT = S.FIN_AID_ST_EXCPT,
    T.TUITION_EXCPT = S.TUITION_EXCPT,
    T.SCC_DISTRICT = S.SCC_DISTRICT,
    T.CITY = S.CITY,
    T.COUNTY = S.COUNTY,
    T.STATE = S.STATE,
    T.COUNTRY = S.COUNTRY,
    T.POSTAL = S.POSTAL,
    T.COMMENTS = S.COMMENTS,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    nvl(trim(T.RESIDENCY_DT),0) <> nvl(trim(S.RESIDENCY_DT),0) or 
    T.RESIDENCY <> S.RESIDENCY or 
    T.ADMISSION_RES <> S.ADMISSION_RES or 
    T.FIN_AID_FED_RES <> S.FIN_AID_FED_RES or 
    T.FIN_AID_ST_RES <> S.FIN_AID_ST_RES or 
    T.TUITION_RES <> S.TUITION_RES or 
    T.ADMISSION_EXCPT <> S.ADMISSION_EXCPT or 
    T.FIN_AID_FED_EXCPT <> S.FIN_AID_FED_EXCPT or 
    T.FIN_AID_ST_EXCPT <> S.FIN_AID_ST_EXCPT or 
    T.TUITION_EXCPT <> S.TUITION_EXCPT or 
    T.SCC_DISTRICT <> S.SCC_DISTRICT or 
    T.CITY <> S.CITY or 
    T.COUNTY <> S.COUNTY or 
    T.STATE <> S.STATE or 
    T.COUNTRY <> S.COUNTRY or 
    T.POSTAL <> S.POSTAL or 
    nvl(trim(T.COMMENTS),0) <> nvl(trim(S.COMMENTS),0) or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.ACAD_CAREER,
    T.INSTITUTION,
    T.EFFECTIVE_TERM, 
    T.SRC_SYS_ID, 
    T.RESIDENCY_DT, 
    T.RESIDENCY,
    T.ADMISSION_RES,
    T.FIN_AID_FED_RES,
    T.FIN_AID_ST_RES, 
    T.TUITION_RES,
    T.ADMISSION_EXCPT,
    T.FIN_AID_FED_EXCPT,
    T.FIN_AID_ST_EXCPT, 
    T.TUITION_EXCPT,
    T.SCC_DISTRICT, 
    T.CITY, 
    T.COUNTY, 
    T.STATE,
    T.COUNTRY,
    T.POSTAL, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID,
    T.COMMENTS
    ) 
values (
    S.EMPLID, 
    S.ACAD_CAREER,
    S.INSTITUTION,
    S.EFFECTIVE_TERM, 
    'CS90', 
    S.RESIDENCY_DT, 
    S.RESIDENCY,
    S.ADMISSION_RES,
    S.FIN_AID_FED_RES,
    S.FIN_AID_ST_RES, 
    S.TUITION_RES,
    S.ADMISSION_EXCPT,
    S.FIN_AID_FED_EXCPT,
    S.FIN_AID_ST_EXCPT, 
    S.TUITION_EXCPT,
    S.SCC_DISTRICT, 
    S.CITY, 
    S.COUNTY, 
    S.STATE,
    S.COUNTRY,
    S.POSTAL, 
    'N',
    'S',
    sysdate,
    sysdate,
    1234,
    S.COMMENTS)
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_RESIDENCY_OFF rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_RESIDENCY_OFF',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_RESIDENCY_OFF';

strSqlCommand := 'commit';
commit;

strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_RESIDENCY_OFF';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_RESIDENCY_OFF';
update AMSTG_OWNER.PS_RESIDENCY_OFF T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, ACAD_CAREER, INSTITUTION, EFFECTIVE_TERM
   from AMSTG_OWNER.PS_RESIDENCY_OFF T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_RESIDENCY_OFF') = 'Y'
  minus
 select EMPLID, ACAD_CAREER, INSTITUTION, nvl(trim(EFFECTIVE_TERM),'-') EFFECTIVE_TERM
   from SYSADM.PS_RESIDENCY_OFF@AMSOURCE S2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_RESIDENCY_OFF') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.ACAD_CAREER = S.ACAD_CAREER
   and T.INSTITUTION = S.INSTITUTION
   and T.EFFECTIVE_TERM = S.EFFECTIVE_TERM
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_RESIDENCY_OFF rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_RESIDENCY_OFF',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_RESIDENCY_OFF'
;

strSqlCommand := 'commit';
commit;

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

EXCEPTION
   WHEN OTHERS
   THEN
      COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION (
         i_SqlCommand   => strSqlCommand,
         i_SqlCode      => SQLCODE,
         i_SqlErrm      => SQLERRM);

END AM_PS_RESIDENCY_OFF_P;
/
