DROP PROCEDURE CSMRT_OWNER.UM_F_FA_STDNT_AWARDS_P
/

--
-- UM_F_FA_STDNT_AWARDS_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_F_FA_STDNT_AWARDS_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads table UM_F_FA_STDNT_AWARDS.
--
--V01   SMT-xxxx 08/03/2018,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_FA_STDNT_AWARDS';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_FA_STDNT_AWARDS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_FA_STDNT_AWARDS';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_AWARDS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_FA_STDNT_AWARDS');

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_AWARDS disable constraint PK_UM_F_FA_STDNT_AWARDS';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_FA_STDNT_AWARDS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_FA_STDNT_AWARDS';
insert /*+ append enable_parallel_dml parallel(8) */ into UM_F_FA_STDNT_AWARDS
  with X as (
select FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID,
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN,
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1900') else EFFDT end) desc) X_ORDER
  from CSSTG_OWNER.PSXLATITEM
 where DATA_ORIGIN <> 'D'),
  CHRG AS (
SELECT /*+ PARALLEL(8) INLINE */
       SETID, CHARGE_PRIORITY, SRC_SYS_ID,
       DESCR,
       ROW_NUMBER() OVER (PARTITION BY SETID, CHARGE_PRIORITY, SRC_SYS_ID ORDER BY EFFDT DESC) ROW_NUM
  FROM CSSTG_OWNER.PS_PMT_CHRG_TBL
 WHERE DATA_ORIGIN <> 'D')
SELECT /*+ PARALLEL(8) INLINE */
       STAID.INSTITUTION_CD,
       NVL(AWD.ACAD_CAREER, '-') ACAD_CAR_CD,
       STAID.AID_YEAR,
       STAID.PERSON_ID,
       NVL(AWD.ITEM_TYPE, '-') ITEM_TYPE,
       STAID.SRC_SYS_ID,
	   nvl(I.INSTITUTION_SID,2147483646) INSTITUTION_SID,
	   nvl(C.ACAD_CAR_SID,2147483646) ACAD_CAR_SID,
       nvl(P.PERSON_SID,2147483646) PERSON_SID,
       nvl(T.ITEM_TYPE_SID, 2147483646) ITEM_TYPE_SID,
       AWD.OFFER_AMOUNT AY_OFFER_AMOUNT,
       AWD.ACCEPT_AMOUNT AY_ACCEPT_AMOUNT,
       AWD.AUTHORIZED_AMOUNT AY_AUTHORIZED_AMOUNT,
       AWD.DISBURSED_AMOUNT AY_DISBURSED_AMOUNT,
       AWD.AWARD_STATUS,
       X1.XLATLONGNAME AWARD_STATUS_LD,     -- XLAT
       AWD.CHARGE_PRIORITY,
       CHRG.DESCR CHARGE_PRIORITY_LD,
       AWD.DISBURSEMENT_PLAN,
       PL.DESCR DISBURSEMENT_PLAN_LD,
       AWD.FA_PROF_JUDGEMENT,
       AWD.LOCK_AWARD_FLAG,
       AWD.PKG_PLAN_ID,
       AWD.PKG_SEQ_NBR,
       AWD.SPLIT_CODE,
       SPL.DESCR SPLIT_CODE_LD,
       AWD.OVERRIDE_NEED,
       AWD.OVERRIDE_FL,
       'N',
       'S',
       sysdate,
       sysdate,
       1234
  FROM CSMRT_OWNER.UM_F_FA_STDNT_AID_ISIR STAID
  left outer join CSSTG_OWNER.PS_STDNT_AWARDS AWD
    ON STAID.PERSON_ID = AWD.EMPLID
   AND STAID.INSTITUTION_CD = AWD.INSTITUTION
   AND STAID.AID_YEAR = AWD.AID_YEAR
   AND STAID.SRC_SYS_ID = AWD.SRC_SYS_ID
   and AWD.DATA_ORIGIN <> 'D'
  LEFT OUTER JOIN CSSTG_OWNER.PS_DISB_SPLIT_CD SPL
    ON AWD.INSTITUTION = SPL.INSTITUTION
   AND AWD.AID_YEAR = SPL.AID_YEAR
   AND AWD.ACAD_CAREER = SPL.ACAD_CAREER
   AND AWD.DISBURSEMENT_PLAN = SPL.DISBURSEMENT_PLAN
   AND AWD.SPLIT_CODE = SPL.SPLIT_CODE
   and SPL.DATA_ORIGIN <> 'D'
  LEFT OUTER JOIN CSSTG_OWNER.PS_DISB_PLAN_TBL PL
    ON AWD.INSTITUTION = PL.INSTITUTION
   AND AWD.AID_YEAR = PL.AID_YEAR
   AND AWD.ACAD_CAREER = PL.ACAD_CAREER
   AND AWD.DISBURSEMENT_PLAN = PL.DISBURSEMENT_PLAN
   and PL.DATA_ORIGIN <> 'D'
  LEFT OUTER JOIN CHRG
    ON AWD.INSTITUTION = CHRG.SETID
   AND AWD.CHARGE_PRIORITY = CHRG.CHARGE_PRIORITY
   AND CHRG.ROW_NUM = 1
  LEFT OUTER JOIN PS_D_INSTITUTION I
    ON STAID.INSTITUTION_CD = I.INSTITUTION_CD
   AND STAID.SRC_SYS_ID = I.SRC_SYS_ID
  left outer join PS_D_ACAD_CAR C
    on AWD.INSTITUTION = C.INSTITUTION_CD
   and AWD.ACAD_CAREER = C.ACAD_CAR_CD
   and AWD.SRC_SYS_ID = C.SRC_SYS_ID
  LEFT OUTER JOIN CSMRT_OWNER.PS_D_PERSON P
    on STAID.PERSON_ID = P.PERSON_ID
   and STAID.SRC_SYS_ID = P.SRC_SYS_ID
   and P.DATA_ORIGIN <> 'D'
  LEFT OUTER JOIN CSMRT_OWNER.UM_D_FA_ITEM_TYPE T
    on AWD.INSTITUTION = T.INSTITUTION_CD
   and AWD.ITEM_TYPE = T.ITEM_TYPE
   and AWD.AID_YEAR = T.AID_YEAR
   and AWD.SRC_SYS_ID = T.SRC_SYS_ID
  left outer join X X1
	  on X1.FIELDNAME = 'AWARD_STATUS'
	 and X1.FIELDVALUE = AWD.AWARD_STATUS
     and X1.X_ORDER = 1
 where STAID.DATA_ORIGIN <> 'D'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_FA_STDNT_AWARDS rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_STDNT_AWARDS',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_STDNT_AWARDS',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_AWARDS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_AWARDS enable constraint PK_UM_F_FA_STDNT_AWARDS';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_FA_STDNT_AWARDS');

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

END UM_F_FA_STDNT_AWARDS_P;
/
