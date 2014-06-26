f="./a"
objs=(opportunities)
#objs=(opportunities offers quotes bookings lineitems assets)
cat ./downloadAllOpps.default.map > ${f}
    if [[ -f ${columnfile} ]]; then
        cat ${columnfile} >> ${f}
    fi


    for coll in "${objs[@]}"
    do
        echo $coll
        echo `cat ${f} | grep "${coll}" | cut -d'|' -f2|sed 's/.keyNameType//g' `
        #TODO, make this support array expression with "'"
        addCols=`cat ${f} | grep "${coll}" | cut -d'|' -f2 | sed 's/.keyNameType//g' | sed "s/\'/\'\'\'/g"| sed 's/$/,/g' | tr -cd "[:print:]" | sed 's/,\+$//' `
#        addCols=`cat ${f} | grep "${coll}" | cut -d'|' -f2 | sed 's/.keyNameType//g' | cut -d'[' -f1 | sed 's/$/,/g' | tr -cd "[:print:]" | sed 's/,\+$//' `
        echo "addCols ==>" $addCols
        # log "Exporting app.${coll} ... with $addCols"

#        mongo ${tenant} --quiet --eval "var tenant='${tenant}';var coll='app.${coll}';var addCols='${addCols}'" exportCollection.js > "${sql_file_path}/${coll}.both.out"

#        if [[ $? -ne 0 ]]; then log "Error in extracting data from Mongo for tenant ${tenant} for app.${coll}"; exit 1; fi

#        grep -v RELATIONSHIPROWS "${sql_file_path}/${coll}.both.out" > "${sql_file_path}/${coll}.out"
#        grep RELATIONSHIPROWS "${sql_file_path}/${coll}.both.out" | cut -d'|' -f2 >> "${sql_file_path}/RELATIONSHIPS.out"
#        rm "${sql_file_path}/${coll}.both.out"
    done
