CREATE OR REPLACE VIEW UM_F_FA_STDNT_AWARDS_VW
BEQUEATH DEFINER
AS 
SELECT INSTITUTION_CD,
           ACAD_CAR_CD,
           AID_YEAR,
           PERSON_ID,
           ITEM_TYPE,
           SRC_SYS_ID,
           INSTITUTION_SID,
           ACAD_CAR_SID,
           PERSON_SID,
           ITEM_TYPE_SID,
           AY_OFFER_AMOUNT,
           AY_ACCEPT_AMOUNT,
           AY_AUTHORIZED_AMOUNT,
           AY_DISBURSED_AMOUNT,
           AWARD_STATUS,
           AWARD_STATUS_LD,
           CHARGE_PRIORITY,
           CHARGE_PRIORITY_LD,
           DISBURSEMENT_PLAN,
           DISBURSEMENT_PLAN_LD,
           FA_PROF_JUDGEMENT,
           LOCK_AWARD_FLAG,
           PKG_PLAN_ID,
           PKG_SEQ_NBR,
           SPLIT_CODE,
           SPLIT_CODE_LD,
           OVERRIDE_NEED,
           OVERRIDE_FL,
           '-' SFA_RPKG_PLAN_ID,
           '-' SFA_EA_INDICATOR
      FROM UM_F_FA_STDNT_AWARDS
--     WHERE ROWNUM < 10000000
;
