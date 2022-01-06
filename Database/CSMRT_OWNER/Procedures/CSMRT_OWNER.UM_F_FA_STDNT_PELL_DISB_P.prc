CREATE OR REPLACE PROCEDURE             "UM_F_FA_STDNT_PELL_DISB_P" AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads table UM_F_FA_STDNT_PELL_DISB.
--
 --V01  SMT-xxxx 07/02/2018,    James Doucette
--                              Converted from SQL Script
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_FA_STDNT_PELL_DISB';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_PELL_DISB';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_FA_STDNT_PELL_DISB');

--alter table UM_F_FA_STDNT_PELL_DISB disable constraint PK_UM_F_FA_STDNT_PELL_DISB;
strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_PELL_DISB disable constraint PK_UM_F_FA_STDNT_PELL_DISB';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_FA_STDNT_PELL_DISB';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_FA_STDNT_PELL_DISB';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );


strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_FA_STDNT_PELL_DISB';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_FA_STDNT_PELL_DISB';				
insert /*+ append */ into UM_F_FA_STDNT_PELL_DISB
with XL as (select /*+ materialize */
                   FIELDNAME, FIELDVALUE, SRC_SYS_ID, XLATLONGNAME, XLATSHORTNAME
              from UM_D_XLATITEM
             where SRC_SYS_ID = 'CS90') 
select /*+ PARALLEL(8) INLINE */
       A.INSTITUTION INSTITUTION_CD, 
       A.AID_YEAR, 
       A.EMPLID PERSON_ID, 
       A.PELL_ORIG_ID, 
       A.PELL_DISB_SEQ_NBR, 
       A.SRC_SYS_ID, 
       I.INSTITUTION_SID, 
       nvl(P.PERSON_SID,2147483646) PERSON_SID, 
       nvl(C.ACAD_CAR_SID,2147483646) ACAD_CAR_SID, 
       nvl(I2.ITEM_TYPE_SID,2147483646) ITEM_TYPE_SID, 
       A.DISBURSEMENT_ID, 
       A.PELL_DISB_AMT, 
       A.PELL_DISB_DT, 
       A.PELL_DISB_STATUS, 
       nvl(X1.XLATSHORTNAME,'-') PELL_DISB_STATUS_SD, 
       nvl(X1.XLATLONGNAME,'-') PELL_DISB_STATUS_LD, 
       A.PELL_DISB_STAT_DT, 
       A.PELL_YTD_DSB_AMT, 
       A.ACTION_CODE, 
       A.PELL_PAYPR_NBR, 
       A.PELL_PAYPR_STRT_DT, 
       A.PELL_PAYPR_END_DT, 
       A.PELL_PAYPR_AMOUNT, 
       A.PELL_PAYPR_COA, 
       A.PELL_PAYPR_ENRL_ST, 
       A.PELL_PAYPR_WEEKS, 
       A.PG_ED_USE_FLAG_1, 
       A.PG_ED_USE_FLAG_2, 
       A.PG_ED_USE_FLAG_3, 
       A.PG_ED_USE_FLAG_4, 
       A.PG_ED_USE_FLAG_5, 
       A.PG_ED_USE_FLAG_6, 
       A.PG_ED_USE_FLAG_7, 
       A.PG_ED_USE_FLAG_8, 
       A.PG_ED_USE_FLAG_9, 
       A.PG_ED_USE_FLAG_10, 
       A.PELL_RFMS_DISB_SEQ, 
       A.PELL_PREV_DISB_REG, 
       A.PELL_DISB_TYPE, 
       A.PELL_COD_DISB_NUM, 
       A.PELL_COD_DISB_SEQ, 
       A.PELL_COD_DISB_AMT, 
       A.PELL_ACT_DISB_DT,
       'N' LOAD_ERROR, 'S' DATA_ORIGIN, SYSDATE CREATED_EW_DTTM, SYSDATE LASTUPD_EW_DTTM, 1234 BATCH_SID
  from CSSTG_OWNER.PS_PELL_DISBMNT A
  join CSMRT_OWNER.PS_D_INSTITUTION I
    on A.INSTITUTION = I.INSTITUTION_CD
   and A.SRC_SYS_ID = I.SRC_SYS_ID
  left outer join CSMRT_OWNER.PS_D_PERSON P
    on A.EMPLID = P.PERSON_ID
   and A.SRC_SYS_ID = P.SRC_SYS_ID
  left outer join CSMRT_OWNER.PS_D_ACAD_CAR C
    on A.INSTITUTION = C.INSTITUTION_CD
   and A.ACAD_CAREER = C.ACAD_CAR_CD
   and A.SRC_SYS_ID = C.SRC_SYS_ID
  left outer join CSMRT_OWNER.PS_D_ITEM_TYPE I2
    on A.INSTITUTION = I2.SETID
   and A.ITEM_TYPE = I2.ITEM_TYPE_ID
   and A.SRC_SYS_ID = I2.SRC_SYS_ID
  left outer join XL X1
    on X1.FIELDNAME = 'PELL_DISB_STATUS'
   and X1.FIELDVALUE = A.PELL_DISB_STATUS 
   and X1.SRC_SYS_ID = A.SRC_SYS_ID
 where A.DATA_ORIGIN <> 'D'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_FA_STDNT_PELL_DISB rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_STDNT_PELL_DISB',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );



strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_STDNT_PELL_DISB',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_PELL_DISB';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
--alter table UM_F_FA_STDNT_PELL_DISB enable constraint PK_UM_F_FA_STDNT_PELL_DISB;

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_PELL_DISB enable constraint PK_UM_F_FA_STDNT_PELL_DISB';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_FA_STDNT_PELL_DISB');

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

END UM_F_FA_STDNT_PELL_DISB_P;
/
