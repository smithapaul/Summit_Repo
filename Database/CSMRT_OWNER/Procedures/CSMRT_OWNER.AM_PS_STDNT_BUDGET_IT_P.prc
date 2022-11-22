DROP PROCEDURE CSMRT_OWNER.AM_PS_STDNT_BUDGET_IT_P
/

--
-- AM_PS_STDNT_BUDGET_IT_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_STDNT_BUDGET_IT_P" IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_STDNT_BUDGET_IT from PeopleSoft table PS_STDNT_BUDGET_IT.
--
-- V01  SMT-xxxx 04/11/2017,    Jim Doucette
--                              Converted from PS_STDNT_BUDGET_IT.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_STDNT_BUDGET_IT';
        intProcessSid                   Integer;
        dtProcessStart                  Date            := SYSDATE;
        strMessage01                    Varchar2(4000);
        strMessage02                    Varchar2(512);
        strMessage03                    Varchar2(512)   :='';
        strNewLine                      Varchar2(2)     := chr(13) || chr(10);
        strSqlCommand                   Varchar2(32767) :='';
        strSqlDynamic                   Varchar2(32767) :='';
        strClientInfo                   Varchar2(100);
        strDELETE_FLG                   Varchar2(1);
        intRowCount                     Integer;
        intTotalRowCount                Integer         := 0;
        intOLD_MAX_SCN                  Integer         := 0;
        intNEW_MAX_SCN                  Integer         := 0;
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
       START_DT = sysdate,
       END_DT = NULL
 where TABLE_NAME = 'PS_STDNT_BUDGET_IT'
;

strSqlCommand := 'commit';
commit;

strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_STDNT_BUDGET_IT@AMSOURCE S)
 where TABLE_NAME = 'PS_STDNT_BUDGET_IT'
;

strSqlCommand := 'commit';
commit;

strMessage01    := 'Selecting variables from AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

select DELETE_FLG, 
       OLD_MAX_SCN, 
       NEW_MAX_SCN
  into strDELETE_FLG,
       intOLD_MAX_SCN,
       intNEW_MAX_SCN
  from AMSTG_OWNER.UM_STAGE_JOBS
 where TABLE_NAME = 'PS_STDNT_BUDGET_IT'
;

strMessage01    := 'Merging data into AMSTG_OWNER.PS_STDNT_BUDGET_IT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_STDNT_BUDGET_IT';
merge /*+ use_hash(S,T) parallel(8) enable_parallel_dml */ into AMSTG_OWNER.PS_STDNT_BUDGET_IT T
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(AID_YEAR),'-') AID_YEAR, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    nvl(trim(STRM),'-') STRM, 
    to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL 
                    else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT, 
    nvl(EFFSEQ,0) EFFSEQ, 
    nvl(trim(BGT_ITEM_CATEGORY),'-') BGT_ITEM_CATEGORY, 
    nvl(trim(BUDGET_ITEM_CD),'-') BUDGET_ITEM_CD, 
    nvl(BUDGET_ITEM_AMOUNT,0) BUDGET_ITEM_AMOUNT, 
    nvl(trim(OPRID),'-') OPRID, 
    nvl(PELL_ITEM_AMOUNT,0) PELL_ITEM_AMOUNT, 
    nvl(SFA_PELITMAMT_LHT,0) SFA_PELITMAMT_LHT
  from SYSADM.PS_STDNT_BUDGET_IT@AMSOURCE S 
-- where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_BUDGET_IT')
 where ORA_ROWSCN > intOLD_MAX_SCN 
   and EMPLID between '00000000' and '99999999'
   and length(EMPLID) = 8 ) S
 on ( 
    T.EMPLID = S.EMPLID and 
    T.INSTITUTION = S.INSTITUTION and 
    T.AID_YEAR = S.AID_YEAR and 
    T.ACAD_CAREER = S.ACAD_CAREER and 
    T.STRM = S.STRM and 
    T.EFFDT = S.EFFDT and 
    T.EFFSEQ = S.EFFSEQ and 
    T.BGT_ITEM_CATEGORY = S.BGT_ITEM_CATEGORY and 
    T.SRC_SYS_ID = 'CS90')
    when matched then update set
    T.BUDGET_ITEM_CD = S.BUDGET_ITEM_CD,
    T.BUDGET_ITEM_AMOUNT = S.BUDGET_ITEM_AMOUNT,
    T.OPRID = S.OPRID,
    T.PELL_ITEM_AMOUNT = S.PELL_ITEM_AMOUNT,
    T.SFA_PELITMAMT_LHT = S.SFA_PELITMAMT_LHT,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.BUDGET_ITEM_CD <> S.BUDGET_ITEM_CD or 
    T.BUDGET_ITEM_AMOUNT <> S.BUDGET_ITEM_AMOUNT or 
    T.OPRID <> S.OPRID or 
    T.PELL_ITEM_AMOUNT <> S.PELL_ITEM_AMOUNT or 
    T.SFA_PELITMAMT_LHT <> S.SFA_PELITMAMT_LHT or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.INSTITUTION,
    T.AID_YEAR, 
    T.ACAD_CAREER,
    T.STRM, 
    T.EFFDT,
    T.EFFSEQ, 
    T.BGT_ITEM_CATEGORY,
    T.SRC_SYS_ID, 
    T.BUDGET_ITEM_CD, 
    T.BUDGET_ITEM_AMOUNT, 
    T.OPRID,
    T.PELL_ITEM_AMOUNT, 
    T.SFA_PELITMAMT_LHT,
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
) 
values (
    S.EMPLID, 
    S.INSTITUTION,
    S.AID_YEAR, 
    S.ACAD_CAREER,
    S.STRM, 
    S.EFFDT,
    S.EFFSEQ, 
    S.BGT_ITEM_CATEGORY,
    'CS90', 
    S.BUDGET_ITEM_CD, 
    S.BUDGET_ITEM_AMOUNT, 
    S.OPRID,
    S.PELL_ITEM_AMOUNT, 
    S.SFA_PELITMAMT_LHT,
    'N',
    'S',
    sysdate,
    sysdate,
    1234);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_STDNT_BUDGET_IT rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_STDNT_BUDGET_IT',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

If strDELETE_FLG = 'Y' then

strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_STDNT_BUDGET_IT';

strSqlCommand := 'commit';
commit;

strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_STDNT_BUDGET_IT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_STDNT_BUDGET_IT';
update /*+ parallel(8) enable_parallel_dml */ AMSTG_OWNER.PS_STDNT_BUDGET_IT T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, INSTITUTION, AID_YEAR, ACAD_CAREER, STRM, EFFDT, EFFSEQ, BGT_ITEM_CATEGORY
   from AMSTG_OWNER.PS_STDNT_BUDGET_IT T2
--  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_BUDGET_IT') = 'Y'
  minus
 select EMPLID, INSTITUTION, AID_YEAR, ACAD_CAREER, STRM, EFFDT, EFFSEQ, BGT_ITEM_CATEGORY
   from SYSADM.PS_STDNT_BUDGET_IT@AMSOURCE S2
--  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_BUDGET_IT') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.INSTITUTION = S.INSTITUTION
   and T.AID_YEAR = S.AID_YEAR
   and T.ACAD_CAREER = S.ACAD_CAREER
   and T.STRM = S.STRM
   and T.EFFDT = S.EFFDT
   and T.EFFSEQ = S.EFFSEQ
   and T.BGT_ITEM_CATEGORY = S.BGT_ITEM_CATEGORY
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_STDNT_BUDGET_IT rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_STDNT_BUDGET_IT',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

End if;

strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_STDNT_BUDGET_IT'
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

END AM_PS_STDNT_BUDGET_IT_P;
/
