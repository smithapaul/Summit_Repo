DROP FUNCTION CSMRT_OWNER.INST_F
/

--
-- INST_F  (Function) 
--
CREATE OR REPLACE function CSMRT_OWNER.inst_f(p_INSTITUTION_CD varchar2) return varchar2
is
 v_dummy varchar2(50);
begin
 select decode(p_INSTITUTION_CD,
               'UMAMH', 'Amherst',
               'UMASS', 'System',
               'UMBOS', 'Boston',
               'UMCEN', 'Central',
               'UMDAR', 'Dartmouth',
               'UMLOW', 'Lowell',
               'UMWOR', 'Worcester',
               'Error') into v_dummy from DUAL;
 return v_dummy;
exception
 when others then return 'Error';
end inst_f;
 
/
