DROP PROCEDURE CSMRT_OWNER.UM_F_FA_AWARD_DISB_P
/

--
-- UM_F_FA_AWARD_DISB_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_F_FA_AWARD_DISB_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads table UM_F_FA_AWARD_DISB.
--
--V01   SMT-xxxx 08/06/2018,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_FA_AWARD_DISB';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_FA_AWARD_DISB';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_FA_AWARD_DISB';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_FA_AWARD_DISB';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_FA_AWARD_DISB');

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_AWARD_DISB disable constraint PK_UM_F_FA_AWARD_DISB';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_FA_AWARD_DISB';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_FA_AWARD_DISB';
insert /*+ append enable_parallel_dml parallel(8) */ into UM_F_FA_AWARD_DISB
WITH
AWDISB AS (
SELECT /*+ PARALLEL(8) INLINE */
       A.EMPLID, A.INSTITUTION, A.AID_YEAR, A.ITEM_TYPE, A.ACAD_CAREER, A.STRM, A.SRC_SYS_ID,
       SUM(A.OFFER_BALANCE) OFFER_BALANCE,
       SUM(A.ACCEPT_BALANCE) ACCEPT_BALANCE,
       SUM(A.AUTHORIZED_BALANCE) AUTHORIZED_BALANCE,
       SUM(A.DISBURSED_BALANCE) DISBURSED_BALANCE,
       SUM(A.NET_DISB_BALANCE) NET_DISB_BALANCE
  FROM CSSTG_OWNER.PS_STDNT_AWRD_DISB A
 where A.DATA_ORIGIN <> 'D'
 GROUP BY A.EMPLID, A.INSTITUTION, A.AID_YEAR, A.ITEM_TYPE, A.ACAD_CAREER, A.STRM, A.SRC_SYS_ID),
ANTICIPATED_AID AS (
SELECT /*+ PARALLEL(8) INLINE */
       INSTITUTION, AID_YEAR, ACAD_CAREER, STRM, EMPLID, ITEM_TYPE, AS_OF_DTTM, SRC_SYS_ID,
       NET_AWARD_AMT,
       ROW_NUMBER() OVER (PARTITION BY INSTITUTION, AID_YEAR, ACAD_CAREER, STRM, EMPLID, ITEM_TYPE, SRC_SYS_ID
                              ORDER BY AS_OF_DTTM DESC, DISBURSEMENT_ID desc, DISBURSEMENT_PLAN desc) DATE_ORDER
  FROM CSSTG_OWNER.PS_ANTICIPATED_AID
 WHERE DATA_ORIGIN <> 'D'
)
SELECT /*+ PARALLEL(8) INLINE */
       STAID.INSTITUTION_CD,
       NVL(AWDISB.ACAD_CAREER, '-') ACAD_CAR_CD,
       STAID.AID_YEAR,
       NVL(AWDISB.STRM, '-') TERM_CD,
       STAID.PERSON_ID,
       NVL(AWDISB.ITEM_TYPE, '-'),
       STAID.SRC_SYS_ID,
	   nvl(I.INSTITUTION_SID,2147483646) INSTITUTION_SID,
       nvl(C.ACAD_CAR_SID,2147483646) ACAD_CAR_SID,
       nvl(T.TERM_SID,2147483646) TERM_SID,
       nvl(P.PERSON_SID,2147483646) PERSON_SID,
       nvl(IT.ITEM_TYPE_SID, 2147483646) ITEM_TYPE_SID,
       AWDISB.OFFER_BALANCE,
       AWDISB.ACCEPT_BALANCE,
       AWDISB.AUTHORIZED_BALANCE,
       AWDISB.DISBURSED_BALANCE,
       AWDISB.NET_DISB_BALANCE,
       NVL(SF_ANT_AID.NET_AWARD_AMT, 0) NET_AWARD_AMT_SF,
	   'N',
	   'S',
	   sysdate,
	   sysdate,
	   1234
  FROM CSMRT_OWNER.UM_F_FA_STDNT_AID_ISIR STAID
  LEFT OUTER JOIN AWDISB
    ON STAID.PERSON_ID = AWDISB.EMPLID
   AND STAID.INSTITUTION_CD = AWDISB.INSTITUTION
   AND STAID.AID_YEAR = AWDISB.AID_YEAR
   AND STAID.SRC_SYS_ID = AWDISB.SRC_SYS_ID
  LEFT OUTER JOIN ANTICIPATED_AID SF_ANT_AID
    ON AWDISB.INSTITUTION = SF_ANT_AID.INSTITUTION
   AND AWDISB.AID_YEAR = SF_ANT_AID.AID_YEAR
   AND AWDISB.ACAD_CAREER = SF_ANT_AID.ACAD_CAREER
   AND AWDISB.EMPLID = SF_ANT_AID.EMPLID
   AND AWDISB.ITEM_TYPE = SF_ANT_AID.ITEM_TYPE
   AND AWDISB.STRM = SF_ANT_AID.STRM
   AND AWDISB.SRC_SYS_ID = SF_ANT_AID.SRC_SYS_ID
   AND SF_ANT_AID.DATE_ORDER = 1
   AND AWDISB.OFFER_BALANCE > 0
   AND AWDISB.DISBURSED_BALANCE < AWDISB.OFFER_BALANCE
  LEFT OUTER JOIN CSMRT_OWNER.PS_D_INSTITUTION I
	on STAID.INSTITUTION_CD = I.INSTITUTION_CD
   and STAID.SRC_SYS_ID = I.SRC_SYS_ID
   and I.DATA_ORIGIN <> 'D'
  LEFT OUTER JOIN CSMRT_OWNER.PS_D_ACAD_CAR C
	on AWDISB.INSTITUTION = C.INSTITUTION_CD
   and AWDISB.ACAD_CAREER = C.ACAD_CAR_CD
   and AWDISB.SRC_SYS_ID = C.SRC_SYS_ID
   and C.DATA_ORIGIN <> 'D'
  LEFT OUTER JOIN CSMRT_OWNER.PS_D_TERM T
	  on AWDISB.INSTITUTION = T.INSTITUTION_CD
	 and AWDISB.ACAD_CAREER = T.ACAD_CAR_CD
	 and AWDISB.STRM = T.TERM_CD
	 and AWDISB.SRC_SYS_ID = T.SRC_SYS_ID
	 and T.DATA_ORIGIN <> 'D'
  LEFT OUTER JOIN CSMRT_OWNER.PS_D_PERSON P
	  ON STAID.PERSON_ID = P.PERSON_ID
	 AND STAID.SRC_SYS_ID = P.SRC_SYS_ID
  LEFT OUTER JOIN CSMRT_OWNER.UM_D_FA_ITEM_TYPE IT
    on AWDISB.INSTITUTION = IT.INSTITUTION_CD
   and AWDISB.ITEM_TYPE = IT.ITEM_TYPE
   and AWDISB.AID_YEAR = IT.AID_YEAR
   and AWDISB.SRC_SYS_ID = IT.SRC_SYS_ID
 where STAID.DATA_ORIGIN <> 'D'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_FA_AWARD_DISB rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_AWARD_DISB',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_AWARD_DISB',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_FA_AWARD_DISB';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_AWARD_DISB enable constraint PK_UM_F_FA_AWARD_DISB';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_FA_AWARD_DISB');

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

END UM_F_FA_AWARD_DISB_P;
/
