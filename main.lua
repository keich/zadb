function printAllGlobals()
    local seen={}
    local function dump(t,i)
        seen[t]=true
        local s={}
        local n=0
        for k, v in pairs(t) do
            n=n+1
            s[n]=tostring(k)
        end
        table.sort(s)
        for k,v in ipairs(s) do
            print(i .. v)
            v=t[v]
            if type(v)=="table" and not seen[v] then
                dump(v,i.."\t")
            end
        end
    end
    dump(_G,"")
end

function dump(o)
    if type(o) == 'table' then
        local s = '{ '
        for k, v in pairs(o) do
           if type(k) ~= 'number' then k = '"' .. k .. '"' end
           s = s .. '[' .. k .. '] = ' .. dump(v) .. ','
        end
        return s .. '} '
    else
        return tostring(o)
    end
end

function todict(data)
    local result = {}
    local nextkey
    print("todict start "..dump(data))
    for i, v in ipairs(data) do
        print("DEBUG "..dump(v))
        if i % 2 == 1 then
            nextkey = v
        else
            result[nextkey] = v
        end
    end
    return result
end

function toResp(data)
    local arraySize = 0
    local t = {}
    for k,v in pairs(data) do
        table.insert(t, "$" .. string.len(k) .. "\r\n" .. k .. "\r\n")
        if "number" == type(v) then
            table.insert(t, ":" .. v .. "\r\n")
        else
            table.insert(t, "$" .. string.len(v) .. "\r\n" .. v .. "\r\n")
        end
        arraySize = arraySize + 2
    end
    local tmp = "*" .. arraySize .. "\r\n" .. table.concat(t, "")
    return tmp
end

function calculate_object_status(objkey)
    local status = 0
    local childcount = 0
    local template = obj_get_field(objkey, "tname")
    if not template or template ~= "NonCrit" then
        local child = rel_get_child("obj.", objkey, "obj.")
        for child_key, from in pairs(child) do
            local c_status = tonumber(obj_get_field(child_key, "status"))
            if c_status and c_status > status then
                status = c_status
            end
            childcount = childcount + 1
        end
        local events = rel_get_child("obj.", objkey, "evt.")
        for evtkey, from in pairs(events) do
            local c_status = tonumber(evt_get_field(evtkey, "status"))
            if c_status  and c_status > status then
                status = c_status
            end
        end
    end
    return status, childcount
end

function update_status(objkey, hist)
    local obj_id = obj_get_field(objkey, "id")
    if obj_id == nil then
        return
    end
    hist[objkey] = 1
    local status, childcount = calculate_object_status(objkey)
    local t = {childcount = childcount}
    obj_add(objkey, t)
    local old_status = tonumber(obj_get_field(objkey, "status"))
    if old_status ~= status then
        local t = {}
        t["status"] = status
        obj_add(objkey, t)
        local parents = rel_get_parents("obj.", objkey, "obj.")
        for parent_key, to in pairs(parents) do
            if hist[parent_key] == nil then
                update_status(parent_key, hist)
            else
                print("circle found " .. objkey .. " to " .. dump(parent_key))
                print(dump(hist))
            end
        end
    end
    hist[objkey] = nil
end

------------------------------------------------------------------------------------
---------------------------------EVENT----------------------------------------------
------------------------------------------------------------------------------------

function evt_get(evtkey)
    return za_db.hgetall("evt.", evtkey)
end

function evt_get_field(evtkey, field)
    return za_db.hget("evt.", evtkey, field)
end

function evt_del(evtkey)
    local parent = rel_get_parents("evt.", evtkey, "obj.")
    for objkey, to in pairs(parent) do
        rel_del("obj.", objkey, "evt.", evtkey)
    end
    za_db.hdelall("evt.", evtkey)
    load_index_del("evt.", evtkey)
end

function evt_add(evtkey, event)
    za_db.hset("evt.", evtkey, event)
    load_index_add("evt.", evtkey)
end

function get_all_events(objkey, out, hist)
    hist[objkey] = 1
    local events = rel_get_child("obj.", objkey, "evt.")
    for event_key, from in pairs(events) do
        out[event_key] = 0
    end
    local child = rel_get_child("obj.", objkey, "obj.")
    for child_key, from in pairs(child) do
        if hist[child_key] == nil then
            get_all_events(child_key, out, hist)
        else
            print("circle found " .. objkey .. " to " .. dump(child_key))
            print(dump(hist))
        end
    end
    hist[objkey] = nil
end

------------------------------------------------------------------------------------
---------------------------------FILTER---------------------------------------------
------------------------------------------------------------------------------------

function filter_get(fltkey)
    return za_db.hgetall("filter.", fltkey)
end

function filter_get_obj(event)
    local db = za_db
    local out = {}
    local match_filters = {}
    for field, field_val in pairs(event) do
        local filters = db.hgetall("filter.index." .. field, field_val)
        for filter_key, v in pairs(filters) do
            match_filters[filter_key] = 0 --TODO use the counter?
        end
    end
    for filter_key, v in pairs(match_filters) do
        local filter = filter_get(filter_key)
        local objects = rel_get_parents("filter.", filter_key, "obj.")
        for object_key, to in pairs(objects) do
            local matched = true
            for field, v in pairs(filter) do
                local field_val = obj_get_field(object_key, field)
                if event[field] ~= field_val then
                    matched = false
                    break
                end
            end
            if matched then
                out[object_key] = ''
            end
        end
    end
    return out
end

function filter_add(objkey, filter)
    local db = za_db
    db.hset("filter.", objkey, filter)
    rel_add("obj.", objkey, "filter.", objkey)
    for field, v in pairs(filter) do
        local field_val = obj_get_field(objkey, field)
        local t = {}
        t[objkey] = ""
        db.hset("filter.index." .. field, field_val, t)
    end
    load_index_add("filter.", objkey)
    return "+OK\r\n"
end

function filter_del(fltkey)
    local db = za_db
    local filter = filter_get(fltkey)
    local parents = rel_get_parents("filter.", fltkey, "obj.")
    for objkey, to in pairs(parents) do
        for field, v in pairs(filter) do
            local field_val = obj_get_field(objkey, field)
            db.hdel("filter.index." .. field, field_val, objkey)
        end
    end
    load_index_del("filter.", fltkey)
    db.hdelall("filter.", fltkey)
end

------------------------------------------------------------------------------------
---------------------------------RELATION-------------------------------------------
------------------------------------------------------------------------------------

function rel_get_child_count(parent_class, parent_key, child_class)
    local child = rel_get_child(parent_class, parent_key, child_class)
    local childcount = 0
    for child_key, from in pairs(child) do
        childcount = childcount + 1
    end
    return childcount
end

function rel_get_parents(child_class, child_key, parent_class)
    local subclass = parent_class .. child_class
    return za_db.hgetall("rel.index.parent." .. subclass, child_key)
end

function rel_get_child(parent_class, parent_key, child_class)
    local subclass = parent_class .. child_class
    return za_db.hgetall("rel.index.child." .. subclass, parent_key)
end

function rel_add(parent_class, parent_key, child_class, child_key)
    local db = za_db
    local subclass = parent_class .. child_class
    local t = {}
    t[child_key] = parent_key
    db.hset("rel.index.child." .. subclass, parent_key, t)

    local t = {}
    t[parent_key] = child_key
    db.hset("rel.index.parent." .. subclass, child_key, t)

    local t = {}
    t["parent_class"] = parent_class
    t["parent_key"] = parent_key
    t["child_class"] = child_class
    t["child_key"] = child_key
    local keylen = string.len(parent_key)
    local relkey = parent_key .. "." .. child_key .. "." .. keylen
    db.hset("rel." .. subclass, relkey, t)

    local hist = {}
    update_status(parent_key, hist)
    load_index_add("rel." .. subclass, relkey)
end

function rel_del(parent_class, parent_key, child_class, child_key)
    local db = za_db
    local subclass = parent_class .. child_class
    db.hdel("rel.index.child." .. subclass, parent_key, child_key)
    db.hdel("rel.index.parent." .. subclass, child_key, parent_key)
    local keylen = string.len(parent_key)
    local relkey = parent_key .. "." .. child_key .. "." .. keylen
    db.hdelall("rel." .. subclass, relkey)
    local hist = {}
    update_status(parent_key, hist)
    load_index_del("rel." .. subclass, relkey)
end

function rel_get(parent_class, child_class, relkey)
    local subclass = parent_class .. child_class
    return za_db.hgetall("rel." .. subclass, relkey)
end

function rel_del_by_dict(keys, parent_class, child_class)
    local count = 0;
    for key, v in pairs(keys) do
        local rel = rel_get(parent_class, child_class, key)
        local parent_key = rel["parent_key"]
        local child_key = rel["child_key"]
        rel_del(parent_class, parent_key, child_class, child_key)
        count = count + 1
    end
    return count
end

------------------------------------------------------------------------------------
---------------------------------OBJECT---------------------------------------------
------------------------------------------------------------------------------------

function obj_get(objkey)
    return za_db.hgetall("obj.", objkey)
end

function obj_get_field(objkey, field)
    return za_db.hget("obj.", objkey, field)
end

function obj_del(objkey)
    --del all relations
    --filter_del(objkey)
    load_index_del("obj.", objkey)
    za_db.hdelall("obj.", objkey)
end

function obj_add( objkey, object)
    za_db.hset("obj.", objkey, object)
    load_index_add("obj.", objkey)
end

------------------------------------------------------------------------------------
---------------------------------LOAD INDEX-----------------------------------------
------------------------------------------------------------------------------------

function load_index_add(class, key)
    local db = za_db
    local now_counter = db.hget("load.index." .. class, "cfg", "now")
    local old_counter = db.hget("load.index." .. class, "cfg", "old")
    if now_counter == nil or old_counter == nil then
        now_counter = 1
        old_counter = 0
        local t = {}
        t["now"] = now_counter
        t["old"] = old_counter
        db.hset("load.index." .. class, "cfg", t)
    end
    local t = {}
    t[key] = 0
    db.hset("load.index." .. class, now_counter, t)
    db.hdel("load.index." .. class, old_counter, key)
end

function load_index_del(class, key)
    local db = za_db
    local now_counter = db.hget("load.index." .. class, "cfg", "now")
    local old_counter = db.hget("load.index." .. class, "cfg", "old")
    if now_counter == nil or old_counter == nil then
        now_counter = 1
        old_counter = 0
    end
    db.hdel("load.index." .. class, now_counter, key)
    db.hdel("load.index." .. class, old_counter, key)
end

function load_index_delold(class)
    local db = za_db
    local now_counter = db.hget("load.index." .. class, "cfg", "now")
    local old_counter = db.hget("load.index." .. class, "cfg", "old")
    if now_counter == nil or old_counter == nil then
        return
    end
    local oldobjs = db.hgetall("load.index." .. class, old_counter)
    local count = 0

    if class == "obj." then
        for key, v in pairs(oldobjs) do
            obj_del(key)
            count = count + 1
        end
    elseif  class == "rel.obj.filter." then
        count = count + rel_del_by_dict(oldobjs, "obj.", "filter.")
    elseif  class == "rel.obj.obj." then
        count = count + rel_del_by_dict(oldobjs, "obj.", "obj.")
    elseif  class == "filter." then
        for key, v in pairs(oldobjs) do
            filter_del(key)
            count = count + 1
        end
    end

    db.hdelall("load.index." .. class, old_counter)
    print("Deleted old objects for " .. class .. ": ", count)
    local t = {}
    t["old"] = now_counter
    t["now"] = now_counter + 1
    db.hset("load.index." .. class, "cfg", t)
    return
end

------------------------------------------------------------------------------------
---------------------------------CMD------------------------------------------------
------------------------------------------------------------------------------------

function resp_relation_add(object)
    local src = object["src_id"]
    local dst = object["dst_id"]
    if src == nil or dst == nil then
        return "+ERR\r\n"
    end
    local src_id = obj_get_field(src, "id")
    if src_id == nil then
        return "+ERR\r\n"
    end
    local dst_id = obj_get_field(dst, "id")
    if dst_id == nil then
        return "+ERR\r\n"
    end
    rel_add("obj.", src ,"obj.",  dst)
    local childcount = rel_get_child_count("obj.", src ,"obj.")
    local t = {}
    t['childcount'] = childcount
    obj_add(src, t)
    return "+OK\r\n"
end

function resp_object_add(object)
    local objkey = object["id"]
    if objkey == nil then
        return "+ERR\r\n"
    end
    obj_add(objkey, object)
    return "+OK\r\n"
end

function resp_object_delold()
    load_index_delold("filter.")
    load_index_delold("obj.")
    load_index_delold("rel.filter.")
    load_index_delold("rel.obj.filter.")
    load_index_delold("rel.obj.obj.")
    return "+OK\r\n"
end

function resp_object_get(key)
    if key == nil then
        return "+ERR\r\n"
    end
    local ret = obj_get(key)
    return toResp(ret)
end

function resp_object_child_get(key)
    if key == nil then
        return "+ERR\r\n"
    end
    local out = {}
    local child = rel_get_child("obj.", key, "obj.")
    local count = 0
    for child_key, from in pairs(child) do
        out[child_key] = 0
        count = count + 1
    end
    print("DEBUG child len " .. count)
    return toResp(out)
end

function resp_event_get(evtkey)
    if evtkey == nil then
        return "+ERR\r\n"
    end
    return toResp(evt_get(evtkey))
end

function resp_event_getall(key)
    if key == nil then
        return "+ERR\r\n"
    end
    local tableNumeration = 0
    local arraySize = 0
    local out = {}
    local hist = {}
    get_all_events(key, out, hist)
    return toResp(out)
end

function resp_event_add(event)
    local evtkey = event["key"]
    if evtkey == nil then
        return "+ERR\r\n"
    end
    event["status"] = tonumber(event["severity"])
    evt_add(evtkey, event)
    local objects = filter_get_obj(event)
    for objkey, v in pairs(objects) do
        rel_add("obj.", objkey, "evt.", evtkey)
    end
    return "+OK\r\n"
end

function resp_event_del(evtkey)
    if evtkey == nil then
        return "+ERR\r\n"
    end
    evt_del(evtkey)
    return "+OK\r\n"
end

function resp_filter_add(objkey, filter)
    if objkey == nil then
        return "+ERR\r\n"
    end
    local obj_id = obj_get_field(objkey, "id")
    if obj_id == nil then
        return "+ERR\r\n"
    end
    filter_add(objkey, filter)
    return "+OK\r\n"
end

print("Start coroutine")

return function(cmdtype, object)
    while true do
        local msg = "+OK\r\n"
        local key = object["key"]
        if cmdtype == "ADDOBJECT" then
            msg = resp_object_add(object)
        elseif cmdtype == "DELOLDOBJECT" then
            msg = resp_object_delold()
        elseif cmdtype == "ADDREL" then
            msg = resp_relation_add(object)
        elseif cmdtype == "GETOBJECT" then
            msg = resp_object_get(key)
        elseif cmdtype == "GETCHILD" then
            msg = resp_object_child_get(key)
        elseif cmdtype == "ADDEVENT" then
            msg = resp_event_add(object)
        elseif cmdtype == "DELEVENT" then
            msg = resp_event_del(key)
        elseif cmdtype == "GETEVENT" then
            msg = resp_event_get(key)
        elseif cmdtype == "GETEVENTSALL" then
            msg = resp_event_getall(key)
        elseif cmdtype == "ADDFILTER" then
            object["key"] = nil
            msg = resp_filter_add(key, object)
        elseif cmdtype == "PRINTALL" then
            za_db.printall();
        elseif cmdtype == "EXIT" then
            return
        elseif cmdtype == "CONNECT" then
            print("New connection , ip is :".. object['name'] .. " , port : ".. object['port'])
            msg = ""
        elseif cmdtype == "DISCONNECT" then
            print("Host disconnected , ip is :".. object['name'] .. " , port : ".. object['port'])
            msg = ""
        end
        cmdtype, object = coroutine.yield(msg)
    end
end
