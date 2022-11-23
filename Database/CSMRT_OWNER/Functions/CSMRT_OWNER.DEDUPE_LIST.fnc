DROP FUNCTION CSMRT_OWNER.DEDUPE_LIST
/

--
-- DEDUPE_LIST  (Function) 
--
CREATE OR REPLACE FUNCTION CSMRT_OWNER.DEDUPE_LIST(
    in_str      varchar2, 
    in_delim    varchar2 default ',')

RETURN varchar2 IS

    v_str       varchar2(32767); 
    v_cur       varchar2(4000); 
    v_prev      varchar2(4000) default '-'; 
    v_start     number default 1; 
    v_length    number default 0; 
    v_end       number := length(in_str); 

BEGIN

    WHILE (case when instr(in_str,in_delim,v_start) = 0 then v_end else v_length end) + v_start < v_end 
     LOOP 
    
        v_length    := instr(in_str,in_delim,v_start) - v_start; 
        v_cur       := substr(in_str,v_start,v_length);

        IF v_cur <> v_prev THEN

            v_str   := v_str || v_cur || in_delim; 
            
        END IF;

        v_prev      := v_cur;
        v_start     := v_start + v_length + 1;
    
    END LOOP;

          IF v_prev <> substr(in_str,v_start) THEN
            
             v_str   := v_str || substr(in_str,v_start);
            
        ELSE v_str   := substr(v_str,1,length(v_str)-1);
        
         END IF; 

    RETURN v_str;  

END DEDUPE_LIST;
/
