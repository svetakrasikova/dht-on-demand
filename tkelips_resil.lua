-------------------------------------------------------------------------------
-- modules and parameters
-------------------------------------------------------------------------------

require"splay.base"
rpc = require"splay.rpc"
misc = require"splay.misc"
crypto = require"crypto"

-- addition to allow local run
PARAMS={}
local cmd_line_args=nil
if not job then --outside the sandbox
	if #arg < 2 then  
		print("lua ", arg[0], " my_position nb_nodes")  
		os.exit()  
	else  		
		local pos, total = tonumber(arg[1]), tonumber(arg[2])  
		local utils = require("splay.utils")
		job = utils.generate_job(pos, total, 20001) 
		cmd_line_args=arg[3]	
	end
end

if arg~=nil then
	if cmd_line_args==nil then cmd_line_args=arg[1] end
	if cmd_line_args~=nil and cmd_line_args~="" then
		print("ARGS: ",cmd_line_args)	
		for _,v in pairs(misc.split(cmd_line_args,":")) do
			local t=misc.split(v,"=")
			PARAMS[t[1]]=t[2]
		end
	end
end


rpc.server(job.me.port)

--size of the network
n = tonumber(arg[2])
--number of affinity groups
k = math.floor(math.sqrt(n))


GOSSIP_TIME= tonumber(PARAMS["GOSSIP_TIME"]) or 5
TKELIPS_RANDOM = tonumber(PARAMS["TKELIPS_RANDOM"]) or 3
TKELIPS_MESSAGE_SIZE = tonumber(PARAMS["TKELIPS_MESSAGE_SIZE"]) or 6
TKELIPS_CONTACT_SIZE = tonumber(PARAMS["TKELIPS_CONTACT_SIZE"]) or 1
TKELIPS_HB_TIMEOUT = tonumber(PARAMS["TKELIPS_HB_TIMEOUT"]) or 25
TKELIPS_AG_SIZE = n/k


-------------------------------------------------------------------------------
-- current node
-------------------------------------------------------------------------------
me = {}
me.peer = job.me
me.age = 0

M = 32
function compute_hash(o)
	return tonumber(string.sub(crypto.evp.new("sha1"):digest(o), 1, M/4), 16)
end

me.id = compute_hash(table.concat({tostring(job.me.ip),":",tostring(job.me.port)}))
-- affinity group
ag = me.id%k


-------------------------------------------------------------------------------
-- T-KELIPS
-------------------------------------------------------------------------------

TKELIPS = {
	aff_group= {},
	contacts = {},
	filetuples = {},
	t = TKELIPS_AG_SIZE,
	c = TKELIPS_CONTACT_SIZE,
	r = TKELIPS_RANDOM,
	m = TKELIPS_MESSAGE_SIZE,
	timeout = TKELIPS_HB_TIMEOUT,
	cycle = 0,
	
	display_view = function(v, which)
 		local display = table.concat({which," VIEW_CONTENT ",me.id,"(", me.id%k,")\t"})
		for i,w in ipairs(v) do
			display = table.concat({display ," ", w.id%k})
		end
		log:print(display)
	end,

	
	display_ag = function(c) 
		local out = table.concat({"TMAN cycle", " ", c, " ", "APPROX. AFFINITY GROUP\t"})
		for i = 1, TKELIPS.t do
			if TKELIPS.aff_group[i] then
				out = table.concat({out, i," ",TKELIPS.aff_group[i].id,"(",TKELIPS.aff_group[i].id%k,")\t"})
			end
		end
		log:print(out)
	end,
	
	display_contacts = function(c)
		local out = table.concat({"TMAN cycle", " ", c, " ", "APPROX. CONTACTS\n"})
		for i = 0, k-1 do
			if TKELIPS.contacts[i] then
			local str = table.concat({i,": "})
				for j = 1, TKELIPS.c do
					if TKELIPS.contacts[i][j] then
						str = table.concat({str," ",TKELIPS.contacts[i][j].id,"(",TKELIPS.contacts[i][j].id%k,")"})
					else
						str = table.concat({str," -"})
					end
				end
			out = table.concat({out,str,"\n"})
			end
		end
		log:print(out)
	end,

	debug = function(c)
		TKELIPS.display_ag(c)
		TKELIPS.display_contacts(c)
	end,
	
-------------------------------------------------------------------------------
-- utilities
-------------------------------------------------------------------------------
	-- remove duplicates from view
	remove_dup = function(set)
		--local rd = misc.time()
		for i,v in ipairs(set) do
			local j = i+1
			while(j <= #set and #set > 0) do
				if v.id == set[j].id then
					table.remove(set,j)
				else j = j + 1
				end
			end
		end
		--log:print("TKELIPS.remove_dup", misc.time()-rd)
	end,

	--keep n first elelements from t
	keep_n = function(t,n)
		for i = #t, n+1, -1 do
			table.remove(t,i)
		end
	end,

	remove_self = function(set, partner)
		for i,v in ipairs(set) do
			if v.peer.port == partner.peer.port and v.peer.ip == partner.peer.ip then table.remove(set, i) end
		end
	end,

	--ranks nodes in set according to the distance on the ring relative to n
	rank = function(n, set)
		local distances = {}
		local ranked = {}
		for i,v in ipairs(set) do
			local d = 0
			local dist_clockwise = math.abs(v.id%k - n.id%k)
			local dist_counter = k - dist_clockwise
			if dist_clockwise <= dist_counter then d = dist_clockwise
			else d = dist_counter end
			distances[#distances+1] = {distance= d, node=v}
		end
		table.sort(distances, function(a,b) return a.distance < b.distance end)
		for i,v in ipairs(distances) do
			ranked[#ranked+1] = v.node
		end
		return ranked
	end,
	
	--ranks nodes in set by their heat beat
	rankByAge = function(set)
		table.sort(set, function(a,b) return a.age < b.age end)
	end,
	
	random_pick = function(n)
		local result = {}
		for i = 1,n do
			repeat index = math.random(1,#job.nodes)
			until (index ~= job.position)
			local a_peer = job.nodes[index]
			local hashed_index = compute_hash(table.concat({tostring(a_peer.ip),":",tostring(a_peer.port)}))
			result[#result+1] = {peer=a_peer, id=hashed_index}
		end
		return result	
	end,
	
		filter_ag = function(received, ag)
		local filtered = {}
		for _,v in ipairs(received) do
			if v.id%k == ag then
				filtered[#filtered+1] = v
			end
		end
		return filtered
	end,
	
	same_node = function(n1,n2)
		if n1.ip==n2.ip and n1.port == n2.port then
			return true
		else
			return false
		end
	end,
	
	
	removeNode = function(node)
		TKELIPS.removeFromAG(node)
		TKELIPS.removefromContacts(node)
	end,
	
	removeFromAG = function(node)
		for i,v in ipairs(TKELIPS.aff_group) do
			if same_node(v, node) then
				table.remove(TKELIPS.aff_group, i)
				break
			end
		end
	end,
	
	removefromContacts = function(node)
		for i = 0, k-1 do
			if TKELIPS.contacts[i] then
				for j = 1, TKELIPS.c do
					if TKELIPS.contacts[i][j] then
						if same_node(TKELIPS.contacts[i][j], node) then
							table.remove(TKELIPS.contacts[i], j)
							break
					end
				end
			end
		end
	end,
	
	removeStaleNodes = function(set)
		for i,v in ipairs(set) do
			if isStale(v) then table.remove(set, i) end
		end
	end,
	
	isStale = function(node)
		if  node.age >= TKELIPS.timeout then return true
		else return false end
	end
	
	updateAge = function(n1,n2)
		if n1.age > n2.age then n1.age = n2.age end
	end
	

-------------------------------------------------------------------------------
-- T-MAN functions
-------------------------------------------------------------------------------

	selectPeer = function()
		local partner = TKELIPS.random_pick(1)[1]
		return partner
		end,

	createMessage = function(partner)
		local merged =  misc.merge(TKELIPS.aff_group, TKELIPS.random_pick(TKELIPS.r))
		TKELIPS.remove_self(merged, partner)
		merged[#merged+1] = me
		TKELIPS.remove_dup(merged)
		merged = TKELIPS.rank(partner, merged)
		TKELIPS.keep_n(merged,TKELIPS.m)
		return merged	
	end,

	updateAffGroup = function(received)
		local ag_candidates = TKELIPS.filter_ag(received, ag)
		local new_entries = {}
		local matched = {}
		local is_new = true
		for i,v in ipairs(ag_candidates) do
			for j,w in ipairs(TKELIPS.aff_group) do
				if same_node(w,v) then
					TKELIPS.updateAge(w,v)
					matched[j] = true
					is_new = false
					break
				end
			end
			if is_new then new_entries[#new_entries+1] = v end
		end
		for i,v in ipairs(TKELIPS.aff_group) do
			if not matched[i] then
				v.age = v.age+1
			end
		end
		local merged = misc.merge(TKELIPS.aff_group, new_entries)		
		TKELIPS.aff_group = merged
		TKELIPS.removeStaleNodes(TKELIPS.aff_group)
	end,

	updateContacts = function(received)
		for i,v in ipairs(received) do
			local ag = v.id%k
			local is_new = true
			if TKELIPS.contacts[ag] == nil then
				TKELIPS.contacts[ag] = {}
			end
			for j,w in TKELIPS.contacts[ag] do
				if same_node(v,w) then
					TKELIPS.updateAge(w,v)
					is_new = false
					break
				end
			end		
			if is_new then
				local t = {v}
				local merged = misc.merge(TKELIPS.contacts[ag], t)
				TKELIPS.rankByAge(merged)
				TKELIPS.keep_n(merged, TKELIPS.c)
				TKELIPS.contacts[ag] = merged
				TKELIPS.removeStaleNodes(TKELIPS.contacts[ag])
			end		
		end
	end,

	passiveThread = function(received,sender)
		local buffer = TKELIPS.createMessage(sender)
		TKELIPS.updateAffGroup(received)
		TKELIPS.updateContacts(received)
		return buffer
	end,

	activeThread = function()
		local partner = TKELIPS.selectPeer()
		local buffer = TKELIPS.createMessage(partner)
		local try = 0
		local ok, res = rpc.acall(partner.peer, {'TKELIPS.passiveThread', buffer, me})
		while not ok do
			try = try + 1
			if try <= 2 then
				log:print("TMAN active thread: no response from:"..partner.id.. ": "..tostring(res).." => try again")
				events.sleep(math.random(try * 3, try * 6))
				ok, res = rpc.acall(partner.peer, {'TKELIPS.passiveThread', buffer, me})
			else
				log:print("TMAN active thread: no response from:"..partner.id..": "..tostring(res).."  => end")
				TKELIPS.removeNode(partner)
			end
		end
		if ok then
			local received = res[1]
			local loc_cycle = TKELIPS.cycle+1
			TKELIPS.cycle = TKELIPS.cycle+1
			TKELIPS.updateAffGroup(received)
			TKELIPS.updateContacts(received)
			TKELIPS.debug(loc_cycle)
		end
	end,
	
}

-------------------------------------------------------------------------------
-- main loop
-------------------------------------------------------------------------------
max_time = 360

function terminator()
  events.sleep(max_time)
  os.exit()
end

function main()
-- this thread will be in charge of killing the node after max_time seconds
	events.thread(terminator)
	log:print("UP: "..job.me.ip..":"..job.me.port)
	log:print(table.concat({"ME: ", me.id, " (", me.id%k, ")"}))
-- init random number generator
	math.randomseed(job.position*os.time())
-- wait for all nodes to start up (conservative)
	events.sleep(2)
-- desynchronize the nodes
	local desync_wait = (GOSSIP_TIME * math.random())
	log:print("waiting for "..desync_wait.." to desynchronize")
	events.sleep(desync_wait) 
  --launching TMAN
	tman_thread = events.periodic(GOSSIP_TIME, TKELIPS.activeThread)
			
end  

events.thread(main)  
events.loop()
