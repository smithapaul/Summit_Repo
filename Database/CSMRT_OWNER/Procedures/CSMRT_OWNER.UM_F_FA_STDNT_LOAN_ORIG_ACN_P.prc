CREATE OR REPLACE PROCEDURE             "UM_F_FA_STDNT_LOAN_ORIG_ACN_P" AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads table UM_F_FA_STDNT_LOAN_ORIG_ACN.
--
 --V01  SMT-xxxx 07/06/2018,    James Doucette
--                              Converted from SQL Script
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_FA_STDNT_LOAN_ORIG_ACN';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_LOAN_ORIG_ACN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_FA_STDNT_LOAN_ORIG_ACN');

--alter table UM_F_FA_STDNT_LOAN_ORIG_ACN disable constraint PK_UM_F_FA_STDNT_LOAN_ORIG_ACN;
strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_LOAN_ORIG_ACN disable constraint PK_UM_F_FA_STDNT_LOAN_ORIG_ACN';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_FA_STDNT_LOAN_ORIG_ACN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_FA_STDNT_LOAN_ORIG_ACN';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );


strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_FA_STDNT_LOAN_ORIG_ACN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_FA_STDNT_LOAN_ORIG_ACN';				
insert /*+ append */ into UM_F_FA_STDNT_LOAN_ORIG_ACN
with XL as (select /*+ materialize */
                   FIELDNAME, FIELDVALUE, SRC_SYS_ID, XLATLONGNAME, XLATSHORTNAME
              from UM_D_XLATITEM
             where SRC_SYS_ID = 'CS90') 
select /*+ PARALLEL(8) INLINE */
       A.INSTITUTION INSTITUTION_CD, A.ACAD_CAREER ACAD_CAR_CD, A.AID_YEAR, A.EMPLID PERSON_ID, A.LOAN_TYPE, A.LN_APPL_SEQ, A.ITEM_TYPE, A.LN_ORIG_ACTN_SEQ, nvl(M.LNORIG_MSG_SEQ,0) LNORIG_MSG_SEQ, A.SRC_SYS_ID, 
       I.INSTITUTION_SID, 
       nvl(C.ACAD_CAR_SID,2147483646) ACAD_CAR_SID, 
       nvl(P.PERSON_SID,2147483646) PERSON_SID, 
       nvl(I2.ITEM_TYPE_SID,2147483646) ITEM_TYPE_SID, 
       nvl(L.DESCRSHORT,'-') LOAN_TYPE_SD, 
       nvl(L.DESCR,'-') LOAN_TYPE_LD, 
       LN_ACTION_CD, 
       LN_ACTION_DT, 
       TRNSFR_BATCH, 
       LN_ACTION_STATUS, 
       nvl(X1.XLATSHORTNAME,'-') LN_ACTION_STATUS_SD, 
       nvl(X1.XLATLONGNAME,'-') LN_ACTION_STATUS_LD, 
       LN_ACTNSTAT_DT, 
       OPRID, 
       PROCESS_INSTANCE, 
       SFA_CR_DOCUMENT_ID, 
       nvl(M.LN_ACTION_MSG,'-') LN_ACTION_MSG, 
       'N' LOAD_ERROR, 'S' DATA_ORIGIN, SYSDATE CREATED_EW_DTTM, SYSDATE LASTUPD_EW_DTTM, 1234 BATCH_SID
  from CSSTG_OWNER.PS_LOAN_ORIG_ACTN A
  left outer join CSSTG_OWNER.PS_LOAN_ORIG_MSG M
    on A.EMPLID = M.EMPLID
   and A.INSTITUTION = M.INSTITUTION
   and A.AID_YEAR = M.AID_YEAR
   and A.ACAD_CAREER = M.ACAD_CAREER
   and A.LOAN_TYPE = M.LOAN_TYPE
   and A.LN_APPL_SEQ = M.LN_APPL_SEQ
   and A.ITEM_TYPE = M.ITEM_TYPE
   and A.LN_ORIG_ACTN_SEQ = M.LN_ORIG_ACTN_SEQ
   and A.SRC_SYS_ID = M.SRC_SYS_ID
   and M.DATA_ORIGIN <> 'D'
  join CSMRT_OWNER.PS_D_INSTITUTION I
    on A.INSTITUTION = I.INSTITUTION_CD
   and A.SRC_SYS_ID = I.SRC_SYS_ID
  left outer join CSMRT_OWNER.PS_D_ACAD_CAR C
    on A.INSTITUTION = C.INSTITUTION_CD
   and A.ACAD_CAREER = C.ACAD_CAR_CD
   and A.SRC_SYS_ID = C.SRC_SYS_ID
  left outer join CSMRT_OWNER.PS_D_PERSON P
    on A.EMPLID = P.PERSON_ID
   and A.SRC_SYS_ID = P.SRC_SYS_ID
  left outer join CSMRT_OWNER.PS_D_ITEM_TYPE I2
    on A.INSTITUTION = I2.SETID
   and A.ITEM_TYPE = I2.ITEM_TYPE_ID
   and A.SRC_SYS_ID = I2.SRC_SYS_ID
  left outer join CSSTG_OWNER.PS_LN_TYPE_TBL L
    on A.INSTITUTION = L.INSTITUTION
   and A.AID_YEAR = L.AID_YEAR
   and A.LOAN_TYPE = L.LOAN_TYPE
   and A.SRC_SYS_ID = L.SRC_SYS_ID
   and L.DATA_ORIGIN <> 'D'
  left outer join XL X1
    on X1.FIELDNAME = 'LN_ACTION_STATUS'
   and X1.FIELDVALUE = A.LN_ACTION_STATUS 
   and X1.SRC_SYS_ID = A.SRC_SYS_ID
 where A.DATA_ORIGIN <> 'D'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_FA_STDNT_LOAN_ORIG_ACN rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_STDNT_LOAN_ORIG_ACN',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );



strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_STDNT_LOAN_ORIG_ACN',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_LOAN_ORIG_ACN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
--alter table UM_F_FA_STDNT_LOAN_ORIG_ACN enable constraint PK_UM_F_FA_STDNT_LOAN_ORIG_ACN;

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_LOAN_ORIG_ACN enable constraint PK_UM_F_FA_STDNT_LOAN_ORIG_ACN';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_FA_STDNT_LOAN_ORIG_ACN');

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

END UM_F_FA_STDNT_LOAN_ORIG_ACN_P;
/
