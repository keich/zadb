
--print lua object to stdout recursive
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


-- return like dictionary from array
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

print("Start coroutine")


return function(cmdtype, object)
    while true do
        local msg = "+OK\r\n"
        if cmdtype == "DOSOMETHING" then
            msg = "+HELLO FROM\r\n"
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

