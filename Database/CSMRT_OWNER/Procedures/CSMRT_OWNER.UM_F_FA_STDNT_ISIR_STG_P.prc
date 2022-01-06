CREATE OR REPLACE PROCEDURE             "UM_F_FA_STDNT_ISIR_STG_P" AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads mart table UM_F_FA_STDNT_ISIR_STG.
--
 --V01  SMT-xxxx 07/12/2018,    James Doucette
--                              Converted from SQL Script
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_FA_STDNT_ISIR_STG';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_ISIR_STG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_FA_STDNT_ISIR_STG');

--alter table UM_F_FA_STDNT_ISIR_STG disable constraint PK_UM_F_FA_STDNT_ISIR_STG;
strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_ISIR_STG disable constraint PK_UM_F_FA_STDNT_ISIR_STG';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_FA_STDNT_ISIR_STG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_FA_STDNT_ISIR_STG';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );


strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_FA_STDNT_ISIR_STG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_FA_STDNT_ISIR_STG';				
insert /*+ APPEND */ into UM_F_FA_STDNT_ISIR_STG
with XL as (select /*+ materialize */
                   FIELDNAME, FIELDVALUE, SRC_SYS_ID, XLATLONGNAME, XLATSHORTNAME
              from UM_D_XLATITEM
             where SRC_SYS_ID = 'CS90') 
select /*+ PARALLEL(8) INLINE */ 
	decode(I1.CPS_SCHOOL_CODE,'002161','UMLOW','002210','UMDAR','002222','UMBOS','-') INSTITUTION_CD,
	I1.EMPLID PERSON_ID, 
	substr(trim(I1.ECTRANSID),-4,4) AID_YEAR, 
	I1.ECQUEUEINSTANCE, 
	--I1.ECTRANSINOUTSW,        -- Always I, need?  
	I1.ISIR_SEQ_NO, 
	I1.SRC_SYS_ID, 
	I.INSTITUTION_SID, 
	nvl(P.PERSON_SID,2147483646) PERSON_SID, 
	I1.ISIR_LOAD_STATUS,   
	nvl(X1.XLATSHORTNAME,'') ISIR_LOAD_STATUS_SD,
	nvl(X1.XLATLONGNAME,'') ISIR_LOAD_STATUS_LD,
	I1.ISIR_LOAD_ACTION,   
	nvl(X2.XLATSHORTNAME,'') ISIR_LOAD_ACTION_SD,
	nvl(X2.XLATLONGNAME,'') ISIR_LOAD_ACTION_LD,
	I1.ADMIT_LVL,   
	nvl(X3.XLATSHORTNAME,'') ADMIT_LVL_SD,
	nvl(X3.XLATLONGNAME,'') ADMIT_LVL_LD,
	I1.ORIG_SSN, 
	I1.SSN, 
	I1.IWD_STD_LAST_NAME, 
	I1.IWD_STD_FIRST_NM02, 
	I1.IWD_STU_MI, 
	I1.IWD_PERM_ADDR02, 
	I1.IWD_CITY, 
	I1.IWD_STATE, 
	I1.IWD_ZIP, 
	I1.BIRTHDATE, 
	I1.IWD_PERM_PHONE, 
	I1.TRANS_RECEIPT_DT, 
	I1.SUSPEND_REASON,   
	nvl(X4.XLATSHORTNAME,'') SUSPEND_REASON_SD,
	nvl(X4.XLATLONGNAME,'') SUSPEND_REASON_LD,
	I2.IWD_TRANS_NBR,
	I2.DEPNDNCY_STAT, 
	nvl(X5.XLATSHORTNAME,'') DEPNDNCY_STAT_SD,
	nvl(X5.XLATLONGNAME,'') DEPNDNCY_STAT_LD,
	I2.TRANS_PROCESS_DT,
	I2.IWD_PRIMARY_EFC,
	I2.IWD_STD_EMAIL,
	I2.IWD_SOURCE_CORR,
	I2.IWD_EFC_CHNG_FLAG,
	I2.ISIR_SAR_C_CHNG,
	'N','S',sysdate,sysdate,1234
from CSSTG_OWNER.PS_ISIR_00_1_EC I1
join CSSTG_OWNER.PS_ISIR_00_2_EC I2
	  on I1.ECTRANSID = I2.ECTRANSID 
	 and I1.ECQUEUEINSTANCE = I2.ECQUEUEINSTANCE
	 and I1.ECTRANSINOUTSW = I2.ECTRANSINOUTSW
	 and I1.ISIR_SEQ_NO = I2.ISIR_SEQ_NO
	 and I1.SRC_SYS_ID = I2.SRC_SYS_ID
	join PS_D_INSTITUTION I
	  on decode(I1.CPS_SCHOOL_CODE,'002161','UMLOW','002210','UMDAR','002222','UMBOS','-') = I.INSTITUTION_CD
	 and I1.SRC_SYS_ID = I.SRC_SYS_ID
	left outer join PS_D_PERSON P
	  on I1.EMPLID = P.PERSON_ID
	 and I1.SRC_SYS_ID = P.SRC_SYS_ID
	left outer join XL X1
	  on X1.FIELDNAME = 'ISIR_LOAD_STATUS'
	 and X1.FIELDVALUE = I1.ISIR_LOAD_STATUS 
	left outer join XL X2
	  on X2.FIELDNAME = 'ISIR_LOAD_ACTION'
	 and X2.FIELDVALUE = I1.ISIR_LOAD_ACTION 
	left outer join XL X3
	  on X3.FIELDNAME = 'ADMIT_LVL'
	 and X3.FIELDVALUE = I1.ADMIT_LVL 
	left outer join XL X4
	  on X4.FIELDNAME = 'SUSPEND_REASON00'
	 and X4.FIELDVALUE = trim(I1.SUSPEND_REASON) 
	left outer join XL X5
	  on X5.FIELDNAME = 'DEPNDNCY_STAT'
	 and X5.FIELDVALUE = I2.DEPNDNCY_STAT 
	where I1.DATA_ORIGIN <> 'D'
	  and I2.DATA_ORIGIN <> 'D'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_FA_STDNT_ISIR_STG rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_STDNT_ISIR_STG',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );



strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_STDNT_ISIR_STG',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_ISIR_STG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
--alter table UM_F_FA_STDNT_ISIR_STG enable constraint PK_UM_F_FA_STDNT_ISIR_STG;

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_ISIR_STG enable constraint PK_UM_F_FA_STDNT_ISIR_STG';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_FA_STDNT_ISIR_STG');

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

END UM_F_FA_STDNT_ISIR_STG_P;
/
