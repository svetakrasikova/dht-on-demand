--[[

Counting the number of mandatory and optional entries in views per second

]]--

misc = require"splay.misc"

if #arg < 3 then
    print("Usage:lua convergence.lua <log.txt> <output.txt> <num_nodes>")
    os.exit()
else
    io.input(arg[1])
		io.output(arg[2])
		num_nodes = tonumber(arg[3])
end


function timeInSec(h,m,s)
t = {wday = 1, year = 2003, yday = 124, month = 5, day = 4, isdst = false, hour = tonumber(h),min= tonumber(m),sec = tonumber(s)}
	return os.time(t)
end


map = {}
complete_states = {}

--parsing the log file line and collecting the number of optional and mandatory entries in the views per second
for line in io.lines() do
	--	parse the timestamp and convert it into seconds
	if first_sec then
		h,m,s = string.match(line, "(%d%d):(%d%d):(%d%d)")
		next_sec = timeInSec(h,m,s) - first_sec
		if next_sec < 0 then sec = 0 else sec = next_sec end
	end
	
	if string.match(line, "VIEW CONSTRUCTION START TIME") then
		h,m,s = string.match(line, "(%d%d):(%d%d):(%d%d)")
		first_sec = timeInSec(h,m,s)
		sec = 0
	end
	
	if sec then 
		if not map[sec] then map[sec] = {} end
	end
	
	--collect complete view states
	if string.match(line, "COMPLETE VIEW STATE") then
		local id, mandatory, optional = string.match(line, "%((%d+)%)%s+COMPLETE VIEW STATE.+mandatory_entries:(%d+).+optional_entries:(%d+)")
		if not complete_states[id] then complete_states[id] = {m = tonumber(mandatory), o = tonumber(optional)} end
	end
	
	--collect view states per second
	if string.match(line, "CURRENT VIEW STATE") then
		local id, mandatory, optional = string.match(line, "%((%d+)%)%s+CURRENT VIEW STATE.+mandatory_entries:(%d+).+optional_entries:(%d+)")
		map[sec][id] = {m = mandatory, o = optional}		
	end
	
	if string.match(line, "BANDWIDTH_TOTAL") then
		local id, tot_KB_sent, tot_KB_recv = string.match(line, "%((%d+)%)%s+BANDWIDTH_TOTAL%s(.+)%s(.+)")
		if map[sec][id] then map[sec][id].bw = tot_KB_sent
		else  map[sec][id] = {bw = tot_KB_sent} end
	end

end


function process_complete_states()
	n = 0
	mand, opt = 0, 0
	for _, v in pairs(complete_states) do
		n = n+1;
		mand = mand + v.m
		opt = opt + v.o
	end
	assert(n==num_nodes)
	return mand, opt
end

-- sorting the map by keys, from Roberto Ierusalimschy "Programming in Lua"
function pairsByKeys(t)
    local a = {}
    for n in pairs(t) do table.insert(a, n) end
    table.sort(a)
    local i = 0      -- iterator variable
    local iter = function ()   -- iterator function
        i = i + 1
        if a[i] == nil then return nil
        else return a[i], t[a[i]]
         end
    end
    return iter
 end
 
function process_view_states(map, mand, opt)
	stats = {}
	for i,v in pairsByKeys(map) do
		if not stats[i] then stats[i] = {m = 0, o = 0, bw = 0}; end
		if previous then
			v = misc.merge(v,previous)
		end
		for j, w in pairs(v) do
			if w.m then stats[i].m = stats[i].m + w.m else stats[i].m = stats[i].m end
			if w.o then stats[i].o = stats[i].o + w.o else stats[i].o = stats[i].o end
			if w.bw then stats[i].bw = stats[i].bw + w.bw else stats[i].bw = stats[i].bw end
		end
		previous = v
		stats[i].m_percent = stats[i].m/(mand/100)
		stats[i].o_percent = stats[i].o/(opt/100)
	end
	return stats
end




complete = {process_complete_states()}
stats = process_view_states(map, complete[1], complete[2])

io.write("SECOND\tMANDATORY\tOPTIONAL\tMANDATORY PERCENTAGE\tOPTIONAL PERCENTAGE\tBANDWIDTH\n")
for i,v in pairsByKeys(stats) do
	io.write(i.."\t"..v.m.."\t"..v.o.."\t"..v.m_percent.."\t"..v.o_percent.."\t"..v.bw.."\n")
end
