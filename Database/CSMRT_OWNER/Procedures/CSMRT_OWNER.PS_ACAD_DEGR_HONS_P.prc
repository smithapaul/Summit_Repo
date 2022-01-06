CREATE OR REPLACE PROCEDURE             "PS_ACAD_DEGR_HONS_P" AUTHID CURRENT_USER IS

/*
-- Run before the first time
DELETE
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_ACAD_DEGR_HONS'

INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
(TABLE_NAME, DELETE_FLG)
VALUES
('PS_ACAD_DEGR_HONS', 'Y')

SELECT *
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_ACAD_DEGR_HONS'
*/


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_ACAD_DEGR_HONS from PeopleSoft table PS_ACAD_DEGR_HONS.
--
 --V01  SMT-xxxx 06/06/2017,    Preethi Lodha
--                              Converted from PS_ACAD_DEGR_HONS.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_ACAD_DEGR_HONS';
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

strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


strSqlCommand   := 'update START_DT on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Reading',
       START_DT = sysdate,
       END_DT = NULL
 where TABLE_NAME = 'PS_ACAD_DEGR_HONS'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_ACAD_DEGR_HONS@SASOURCE S)
 where TABLE_NAME = 'PS_ACAD_DEGR_HONS'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_ACAD_DEGR_HONS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_ACAD_DEGR_HONS';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_ACAD_DEGR_HONS T
using (select /*+ full(S) */
nvl(trim(EMPLID),'-') EMPLID,
nvl(trim(STDNT_DEGR),'-') STDNT_DEGR,
nvl(HONORS_NBR,0) HONORS_NBR,
nvl(trim(HONORS_CODE),'-') HONORS_CODE,
to_date(to_char(case when HONORS_AWARD_DT < '01-JAN-1800' then NULL else HONORS_AWARD_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') HONORS_AWARD_DT,
nvl(trim(DIPLOMA_PRINT_FL),'-') DIPLOMA_PRINT_FL,
nvl(trim(TRNSCR_PRINT_FL),'-') TRNSCR_PRINT_FL,
nvl(trim(OPRID),'-') OPRID
from SYSADM.PS_ACAD_DEGR_HONS@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_DEGR_HONS') ) S
   on (
T.EMPLID = S.EMPLID and
T.STDNT_DEGR = S.STDNT_DEGR and
T.HONORS_NBR = S.HONORS_NBR and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.HONORS_CODE = S.HONORS_CODE,
T.HONORS_AWARD_DT = S.HONORS_AWARD_DT,
T.DIPLOMA_PRINT_FL = S.DIPLOMA_PRINT_FL,
T.TRNSCR_PRINT_FL = S.TRNSCR_PRINT_FL,
T.OPRID = S.OPRID,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.HONORS_CODE <> S.HONORS_CODE or
nvl(trim(T.HONORS_AWARD_DT),0) <> nvl(trim(S.HONORS_AWARD_DT),0) or
T.DIPLOMA_PRINT_FL <> S.DIPLOMA_PRINT_FL or
T.TRNSCR_PRINT_FL <> S.TRNSCR_PRINT_FL or
T.OPRID <> S.OPRID or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.EMPLID,
T.STDNT_DEGR,
T.HONORS_NBR,
T.SRC_SYS_ID,
T.HONORS_CODE,
T.HONORS_AWARD_DT,
T.DIPLOMA_PRINT_FL,
T.TRNSCR_PRINT_FL,
T.OPRID,
T.LOAD_ERROR,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM,
T.BATCH_SID
)
values (
S.EMPLID,
S.STDNT_DEGR,
S.HONORS_NBR,
'CS90',
S.HONORS_CODE,
S.HONORS_AWARD_DT,
S.DIPLOMA_PRINT_FL,
S.TRNSCR_PRINT_FL,
S.OPRID,
'N',
'S',
sysdate,
sysdate,
1234);

commit;


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ACAD_DEGR_HONS rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ACAD_DEGR_HONS',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_ACAD_DEGR_HONS';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_ACAD_DEGR_HONS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_ACAD_DEGR_HONS';
update CSSTG_OWNER.PS_ACAD_DEGR_HONS T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, STDNT_DEGR, HONORS_NBR
   from CSSTG_OWNER.PS_ACAD_DEGR_HONS T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_DEGR_HONS') = 'Y'
  minus
 select EMPLID, STDNT_DEGR,HONORS_NBR
   from SYSADM.PS_ACAD_DEGR_HONS@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_DEGR_HONS') = 'Y' 

   ) S
 where T.EMPLID = S.EMPLID
   and T.STDNT_DEGR = S.STDNT_DEGR
   AND T.HONORS_NBR = S.HONORS_NBR
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ACAD_DEGR_HONS rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ACAD_DEGR_HONS',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_ACAD_DEGR_HONS'
;

strSqlCommand := 'commit';
commit;


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

END PS_ACAD_DEGR_HONS_P;
/
