--counting the number of mandatory and optional entries in TKELIPS affinity group views and contacts per time interval
--usage: <log.txt> <output.txt> <num_nodes>
misc = require"splay.misc"

io.input(arg[1])
io.output(arg[2])


num_nodes = arg[3]
map = {}
complete_states = {}

for line in io.lines() do
	--	parse the timestamp and convert it into seconds 
	h,m,s = string.match(line, "(%d%d):(%d%d):(%d%d)")
	if sec == nil then
		first_sec = timeInSec(h,m,s)
		sec = 0
	else sec = timeInSec(h,m,s) - first_sec end
	if not map[sec] then map[sec] = {} end
	
	--collect complete view states
	if string.match(line, "COMPLETE VIEW STATE") then
		local id, mandatory, optional = string.match(line, "STATE.-(%d+).+mandatory_entries:(%d+).+optional_entries:(%d+)")
		if not complete_states[id] then complete_states[id] = {m = tonumber(mandatory), o = tonumber(optional)}
	end
	
	--collect view states per second
	if string.match(line, "CURRENT VIEW STATE") then
		local id, mandatory, optional = string.match(line, "STATE.-(%d+).+mandatory_entries:(%d+).+optional_entries:(%d+)")
		map[sec].id = {m = mandatory, o = optional}
	end
	
end

function timeInSec(h,m,s)
t = {wday = 1, year = 2003, yday = 124, month = 5, day = 4, isdst = false, hour = tonumber(h),min= tonumber(m),sec = tonumber(s)}
	return os.time(t)
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


function process_view_states(map, mand, opt)
	stats = {}
	for i,v in pairs(map) do
		stats[i] = {m = 0, o = 0}
		if previous then
			v = misc.merge(v,previous)
		end
		for _, w in pairs(v) do
			stats[i].m = stats[i].m + w.m
			stats[i].o = stats[i].o + w.o
		end
		previous = v
		stats[i].m_percent = stats[i].m/(mand/100)
		stats[i].o_percent = stats[i].o/(opt/100)
	end
	return stats
end

complete = process_complete_states()
stats = process_complete_states(map, complete[1], complete[2])
io.write("SECOND\tMANDATORY\tOPTIONAL\tMANDATORY PERCENTAGE\tOPTIONAL PERCENTAGE")
for i,v in pairs(stats) do
	io.write(i.."\t"..v.m.."\t"..v.o.."\t"..v.m_percent.."\t"..v.o_percent.."\n")
end
