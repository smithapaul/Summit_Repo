CREATE OR REPLACE PROCEDURE             "UM_F_SF_BILLING_P" AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table UM_F_SF_BILLING from PeopleSoft table UM_F_SF_BILLING.
--
 --V01  SMT-xxxx 06/28/2018,    James Doucette
--                              Converted from SQL Script
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_SF_BILLING';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_SF_BILLING';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_SF_BILLING');

--alter table UM_F_SF_BILLING disable constraint PK_UM_F_SF_BILLING;
strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_SF_BILLING disable constraint PK_UM_F_SF_BILLING';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_SF_BILLING';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_SF_BILLING';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );


strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_SF_BILLING';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_SF_BILLING';				
insert /*+ append */ into UM_F_SF_BILLING
with 
XL as (
select /*+ materialize */
       FIELDNAME, FIELDVALUE, SRC_SYS_ID, XLATLONGNAME, XLATSHORTNAME
  from UM_D_XLATITEM
 where SRC_SYS_ID = 'CS90')
select /*+ parallel(8) inline */
       H.BUSINESS_UNIT INSTITUTION_CD, 
       H.INVOICE_ID, 
       nvl(L.ITEM_NBR,'-') ITEM_NBR, 
       nvl(L.ITEM_LINE,0) ITEM_LINE, 
       H.SRC_SYS_ID,
       H.EMPLID PERSON_ID,
       nvl(I.INSTITUTION_SID,2147483646) INSTITUTION_SID,
       nvl(C.ACAD_CAR_SID,2147483646) ACAD_CAR_SID,
       nvl(P.PERSON_SID,2147483646) PERSON_SID,
       nvl(LD.ACAD_LOAD_SID,2147483646) ACAD_LOAD_SID,  
       nvl(LV.ACAD_LVL_SID,2147483646) ACAD_LVL_PROJ_SID, 
       nvl(G1.ACAD_PROG_SID,2147483646) ACAD_PROG_PRIM_SID, 
       nvl(G2.ACAD_PROG_SID,2147483646) ACAD_PROG_SID, 
       nvl(PL.ACAD_PLAN_SID,2147483646) ACAD_PLAN_SID,
       nvl(CA.CAMPUS_SID,2147483646) CAMPUS_SID,  
       H.NAME,
       H.BI_REQ_NBR,      
       H.BILL_DATE_TIME,
       H.BILL_REQ_ID, 
       H.DUE_DT,        
       H.INVOICE_DT,     
       H.OPRID, 
       H.PRIOR_INVOICE_ID,
       H.PRT_BILL_FLAG, 
       H.PRT_DTTM_STAMP,  
       H.RE_PRT_BILL_FLAG, 
       H.RE_PRT_DTTM_STAMP,  
       H.SF_BILL_STATUS,   
       nvl(X1.XLATLONGNAME,'-') SF_BILL_STATUS_LD,
       H.SSF_ERROR_WARN, 
       nvl(X2.XLATLONGNAME,'-') SSF_ERROR_WARN_LD,
       H.TUITION_RES,           -- RSDNCY SID??? 
       H.TOTAL_BILL,
       H.PRIOR_IVC_BALANCE, 
       H.ADDRESS1, 
       H.ADDRESS2, 
       H.ADDRESS3, 
       H.ADDRESS4, 
       H.CITY, 
       H.COUNTY, 
       H.STATE,                 -- Add state names, _SD, _LD???
       H.POSTAL, 
       H.COUNTRY,               -- Add country names, _SD, _LD??? 
       H.SCC_ROW_ADD_OPRID,  
       H.SCC_ROW_ADD_DTTM,  
       H.SCC_ROW_UPD_OPRID,  
       H.SCC_ROW_UPD_DTTM,
       'N' LOAD_ERROR,
       'S' DATA_ORIGIN,
       SYSDATE CREATED_EW_DTTM,
       SYSDATE LASTUPD_EW_DTTM,
       1234 BATCH_SID
  from CSSTG_OWNER.PS_BI_BILL_HEADER H
  left outer join CSSTG_OWNER.PS_BI_BILLING_LINE L
    on H.BUSINESS_UNIT = L.BUSINESS_UNIT
   and H.INVOICE_ID = L.INVOICE_ID
   and L.DATA_ORIGIN <> 'D'
  left outer join XL X1
    on X1.FIELDNAME = 'SF_BILL_STATUS'
   and X1.FIELDVALUE = H.SF_BILL_STATUS 
   and X1.SRC_SYS_ID = H.SRC_SYS_ID
  left outer join XL X2
    on X2.FIELDNAME = 'SSF_ERROR_WARN'
   and X2.FIELDVALUE = H.SSF_ERROR_WARN 
   and X2.SRC_SYS_ID = H.SRC_SYS_ID
  left outer join CSMRT_OWNER.PS_D_INSTITUTION I
    on H.BUSINESS_UNIT = I.INSTITUTION_CD
   and H.SRC_SYS_ID = I.SRC_SYS_ID
   and I.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_ACAD_CAR C
    on H.BUSINESS_UNIT = C.INSTITUTION_CD
   and H.ACAD_CAREER = C.ACAD_CAR_CD
   and H.SRC_SYS_ID = C.SRC_SYS_ID
   and C.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_PERSON P
    on H.EMPLID = P.PERSON_ID
   and H.SRC_SYS_ID = P.SRC_SYS_ID
   and P.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_ACAD_LOAD LD  
    on H.ACADEMIC_LOAD = LD.ACAD_LOAD_CD 
   and LD.APPRVD_IND = 'Y'
   and H.SRC_SYS_ID = LD.SRC_SYS_ID
   and LD.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_ACAD_LVL LV  
    on H.ACAD_LEVEL_PROJ = LV.ACAD_LVL_CD 
   and H.SRC_SYS_ID = LV.SRC_SYS_ID
   and LV.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.UM_D_ACAD_PROG G1
    on H.BUSINESS_UNIT = G1.INSTITUTION_CD
   and H.ACAD_PROG_PRIMARY = G1.ACAD_PROG_CD
   and H.SRC_SYS_ID = G1.SRC_SYS_ID
   and G1.EFFDT_ORDER = 1 
   and G1.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.UM_D_ACAD_PROG G2
    on H.BUSINESS_UNIT = G2.INSTITUTION_CD
   and H.ACAD_PROG = G2.ACAD_PROG_CD
   and H.SRC_SYS_ID = G2.SRC_SYS_ID
   and G2.EFFDT_ORDER = 1 
   and G2.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.UM_D_ACAD_PLAN PL
    on H.BUSINESS_UNIT = PL.INSTITUTION_CD
   and H.ACAD_PLAN = PL.ACAD_PLAN_CD
   and H.SRC_SYS_ID = PL.SRC_SYS_ID
   and PL.EFFDT_ORDER = 1 
   and PL.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_CAMPUS CA
    on H.BUSINESS_UNIT = CA.INSTITUTION_CD 
   and H.CAMPUS = CA.CAMPUS_CD
   and H.SRC_SYS_ID = CA.SRC_SYS_ID
   and CA.DATA_ORIGIN <> 'D'
 where H.DATA_ORIGIN <> 'D'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_SF_BILLING rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_SF_BILLING',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );



strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_SF_BILLING',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_SF_BILLING';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
--alter table UM_F_SF_BILLING enable constraint PK_UM_F_SF_BILLING;

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_SF_BILLING enable constraint PK_UM_F_SF_BILLING';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_SF_BILLING');

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

END UM_F_SF_BILLING_P;
/
