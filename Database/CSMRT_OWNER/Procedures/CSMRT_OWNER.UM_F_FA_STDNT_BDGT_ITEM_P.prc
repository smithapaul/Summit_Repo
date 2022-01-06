CREATE OR REPLACE PROCEDURE             "UM_F_FA_STDNT_BDGT_ITEM_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads table UM_F_FA_STDNT_BDGT_ITEM.
--
--V01   SMT-xxxx 08/20/2018,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_FA_STDNT_BDGT_ITEM';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_BDGT_ITEM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_FA_STDNT_BDGT_ITEM');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_BDGT_ITEM disable constraint PK_UM_F_FA_STDNT_BDGT_ITEM';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_FA_STDNT_BDGT_ITEM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_FA_STDNT_BDGT_ITEM';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_FA_STDNT_BDGT_ITEM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_FA_STDNT_BDGT_ITEM';				
insert /*+ append */ into UM_F_FA_STDNT_BDGT_ITEM 
  with X as (  
select FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID, 
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN, 
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1900') else EFFDT end) desc) X_ORDER
  from CSSTG_OWNER.PSXLATITEM
 where DATA_ORIGIN <> 'D'),
TERM_BDGT_ITEM AS (
SELECT /*+ PARALLEL(8) INLINE */ 
       EMPLID PERSON_ID, INSTITUTION INSTITUTION_CD, AID_YEAR, ACAD_CAREER ACAD_CAR_CD, STRM TERM_CD, EFFDT, EFFSEQ, BGT_ITEM_CATEGORY, SRC_SYS_ID,
       BUDGET_ITEM_CD, BUDGET_ITEM_AMOUNT, OPRID, 
       PELL_ITEM_AMOUNT, SFA_PELITMAMT_LHT, 
       RANK() OVER (PARTITION BY EMPLID, INSTITUTION, ACAD_CAREER, STRM, AID_YEAR ORDER BY EFFDT DESC, EFFSEQ DESC) IT_ORDER
  FROM CSSTG_OWNER.PS_STDNT_BUDGET_IT 
 WHERE DATA_ORIGIN <> 'D') 
SELECT /*+ PARALLEL(8) INLINE */
       STAID.INSTITUTION_CD, 
       NVL(TERM_BDGT_ITEM.ACAD_CAR_CD, '-') ACAD_CAR_CD,
       STAID.AID_YEAR, 
       NVL(TERM_BDGT_ITEM.TERM_CD, '-') TERM_CD, 
       STAID.PERSON_ID,    
       NVL(TERM_BDGT_ITEM.BGT_ITEM_CATEGORY, '-') BGT_ITEM_CATEGORY,
       STAID.SRC_SYS_ID, 
       TERM_BDGT_ITEM.EFFDT, 
       TERM_BDGT_ITEM.EFFSEQ, 
       nvl(I.INSTITUTION_SID,2147483646) INSTITUTION_SID,   
       nvl(C.ACAD_CAR_SID,2147483646) ACAD_CAR_SID,   
       nvl(T.TERM_SID,2147483646) TERM_SID,   
       nvl(P.PERSON_SID,2147483646) PERSON_SID,
       CAT_TBL.DESCR BGT_ITEM_CATEGORY_LD,
       TERM_BDGT_ITEM.BUDGET_ITEM_CD,
       ITEM_TBL.DESCR BUDGET_ITEM_CD_LD, 
       TERM_BDGT_ITEM.BUDGET_ITEM_AMOUNT, 
       TERM_BDGT_ITEM.OPRID, 
       TERM_BDGT_ITEM.PELL_ITEM_AMOUNT, 
       TERM_BDGT_ITEM.SFA_PELITMAMT_LHT,
       'N' LOAD_ERROR,
       'S' DATA_ORIGIN,
       SYSDATE CREATED_EW_DTTM,
       SYSDATE LASTUPD_EW_DTTM,
       1234 BATCH_SID 
  FROM CSMRT_OWNER.UM_F_FA_STDNT_AID_ISIR STAID 
  LEFT OUTER JOIN TERM_BDGT_ITEM 
    ON STAID.PERSON_ID = TERM_BDGT_ITEM.PERSON_ID 
   AND STAID.INSTITUTION_CD = TERM_BDGT_ITEM.INSTITUTION_CD 
   AND STAID.AID_YEAR = TERM_BDGT_ITEM.AID_YEAR 
   AND TERM_BDGT_ITEM.IT_ORDER = 1
  LEFT OUTER JOIN CSSTG_OWNER.PS_BUDGET_CATG_TBL CAT_TBL 
    ON STAID.INSTITUTION_CD = CAT_TBL.INSTITUTION 
   AND STAID.AID_YEAR = CAT_TBL.AID_YEAR 
   AND nvl(TERM_BDGT_ITEM.BGT_ITEM_CATEGORY,'-') = CAT_TBL.BGT_ITEM_CATEGORY
   AND STAID.SRC_SYS_ID = CAT_TBL.SRC_SYS_ID
  LEFT OUTER JOIN CSSTG_OWNER.PS_BUDGET_ITEM_TBL ITEM_TBL 
    ON STAID.INSTITUTION_CD = ITEM_TBL.INSTITUTION 
   AND STAID.AID_YEAR = ITEM_TBL.AID_YEAR 
   AND TERM_BDGT_ITEM.BGT_ITEM_CATEGORY = ITEM_TBL.BGT_ITEM_CATEGORY  
   AND TERM_BDGT_ITEM.BUDGET_ITEM_CD = ITEM_TBL.BUDGET_ITEM_CD       
   AND STAID.SRC_SYS_ID = ITEM_TBL.SRC_SYS_ID
  LEFT OUTER JOIN CSMRT_OWNER.PS_D_INSTITUTION I  
    on STAID.INSTITUTION_CD = I.INSTITUTION_CD
   and STAID.SRC_SYS_ID = I.SRC_SYS_ID
   and I.DATA_ORIGIN <> 'D'
  LEFT OUTER JOIN CSMRT_OWNER.PS_D_ACAD_CAR C  
    on STAID.INSTITUTION_CD = C.INSTITUTION_CD
   and TERM_BDGT_ITEM.ACAD_CAR_CD = C.ACAD_CAR_CD
   and STAID.SRC_SYS_ID = C.SRC_SYS_ID
   and C.DATA_ORIGIN <> 'D'
  LEFT OUTER JOIN CSMRT_OWNER.PS_D_TERM T  
    on STAID.INSTITUTION_CD = T.INSTITUTION_CD
   and TERM_BDGT_ITEM.ACAD_CAR_CD = T.ACAD_CAR_CD
   and TERM_BDGT_ITEM.TERM_CD = T.TERM_CD
   and STAID.SRC_SYS_ID = T.SRC_SYS_ID
  LEFT OUTER JOIN CSMRT_OWNER.PS_D_PERSON P
    ON STAID.PERSON_ID = P.PERSON_ID
	 AND STAID.SRC_SYS_ID = P.SRC_SYS_ID
 where STAID.DATA_ORIGIN <> 'D'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_FA_STDNT_BDGT_ITEM rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_STDNT_BDGT_ITEM',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_STDNT_BDGT_ITEM',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_BDGT_ITEM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_BDGT_ITEM enable constraint PK_UM_F_FA_STDNT_BDGT_ITEM';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_FA_STDNT_BDGT_ITEM');

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

END UM_F_FA_STDNT_BDGT_ITEM_P;
/
