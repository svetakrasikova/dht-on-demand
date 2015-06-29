-------------------------------------------------------------------------------
-- modules and parameters
-------------------------------------------------------------------------------

require"splay.base"
rpc = require"splay.rpc"
rpc.l_o.level=1
misc = require"splay.misc"
crypto = require"crypto"
socket = require"socket.core"

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
	
	rs=require"splay.restricted_socket"
	rs.init(
		{max_sockets=1024}
	)
	--already wrapped in sandbox, useful
	--for local runs with restrictions	
	socket=rs.wrap(socket)
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
n = arg[2]
--number of affinity groups
k = math.floor(math.sqrt(n))


GOSSIP_TIME= tonumber(PARAMS["GOSSIP_TIME"]) or 5
TMAN_RANDOM = tonumber(PARAMS["TMAN_RANDOM"]) or 3
TMAN_MESSAGE_SIZE = tonumber(PARAMS["TMAN_MESSAGE_SIZE"]) or 6
TMAN_CONTACT_SIZE = tonumber(PARAMS["TMAN_CONTACT_SIZE"]) or 1
TMAN_AG_SIZE = n/k


-------------------------------------------------------------------------------
-- current node
-------------------------------------------------------------------------------
me = {}
me.peer = job.me

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

TMAN = {
	aff_group= {},
	contacts = {},
	filetuples = {},
	t = TMAN_AG_SIZE,
	c = TMAN_CONTACT_SIZE,
	r = TMAN_RANDOM,
	m = TMAN_MESSAGE_SIZE,
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
		for i = 1, TMAN.t do
			if TMAN.aff_group[i] then
				out = table.concat({out, i," ",TMAN.aff_group[i].id,"(",TMAN.aff_group[i].id%k,")\t"})
			end
		end
		log:print(out)
	end,
	
	display_contacts = function(c)
		local out = table.concat({"TMAN cycle", " ", c, " ", "APPROX. CONTACTS\n"})
		for i = 0, k-1 do
			if TMAN.contacts[i] then
			local str = table.concat({i,": "})
				for j = 1, TMAN.c do
					if TMAN.contacts[i][j] then
						str = table.concat({str," ",TMAN.contacts[i][j].id,"(",TMAN.contacts[i][j].id%k,")"})
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
		TMAN.display_ag(c)
		--TMAN.display_contacts(c)
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
		--log:print("TMAN.remove_dup", misc.time()-rd)
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

-------------------------------------------------------------------------------
-- T-MAN functions
-------------------------------------------------------------------------------

	selectPeer = function()
		local partner = TMAN.random_pick(1)[1]
		return partner
		end,

	createMessage = function(partner)	
		--local cm = misc.time()
		local merged =  misc.merge(TMAN.aff_group, TMAN.random_pick(TMAN.r))
		TMAN.remove_self(merged, partner)
		merged[#merged+1] = me
		TMAN.remove_dup(merged)
		merged = TMAN.rank(partner, merged)
		TMAN.keep_n(merged,TMAN.m)
		--log:print("TMAN.createMessage", misc.time() - cm)
		return merged	
	end,


--update affinity group using received 
	updateAffGroup = function(received)
		local ag_candidates = TMAN.filter_ag(received, ag)
		local merged = misc.merge(TMAN.aff_group, ag_candidates)
		TMAN.remove_dup(merged)
		TMAN.aff_group = merged
	end,

--update contacts using received
	updateContacts = function(received)
		for i,v in ipairs(received) do
			local ag = v.id%k
			if TMAN.contacts[ag] == nil then
				TMAN.contacts[ag] = {}
			end
			if #TMAN.contacts[ag] < TMAN.c then
				local t = {v}
				local merged = misc.merge(TMAN.contacts[ag],t)
				TMAN.remove_dup(merged)
				TMAN.keep_n(merged,TMAN.c)
				TMAN.contacts[ag] = merged
			end
		end
	end,

	passiveThread = function(received,sender)
		local buffer = TMAN.createMessage(sender)
		TMAN.updateAffGroup(received)
		TMAN.updateContacts(received)
		return buffer
	end,

	activeThread = function()
		local partner = TMAN.selectPeer()
		--print("Selected partner: ", partner.id);
		local buffer = TMAN.createMessage(partner)
		--print("Active thread, buffer: ")
		--for i=1, #buffer do
			--if buffer[i] then print(buffer[i].id%k, " ") end
		--end
		local try = 0
		local ok, res = rpc.acall(partner.peer, {'TMAN.passiveThread', buffer, me})
		while not ok do
			try = try + 1
			if try <= 3 then
				log:print("TMAN active thread: no response from:"..partner.id.. ": "..tostring(res).." => try again")
				events.sleep(math.random(try * 3, try * 6))
				ok, res = rpc.acall(partner.peer, {'TMAN.passiveThread', buffer, me})
			else
				log:print("TMAN active thread: no response from:"..partner.id..": "..tostring(res).."  => end")
			end
		end
		if ok then
			local received = res[1]
			local loc_cycle = TMAN.cycle +1
			TMAN.cycle = TMAN.cycle + 1
			TMAN.updateAffGroup(received)
			TMAN.updateContacts(received)
			TMAN.debug(loc_cycle)
		end
	end,
	
}

-------------------------------------------------------------------------------
-- key insertion and lookup
-------------------------------------------------------------------------------
KELIPS_LOOKUP = {
	
	display_ft = function()
		local out = table.concat({"FILETUPLES ", me.id, "(", me.id%k, ")\n"})
		for i,v in pairs(TMAN.filetuples) do
			out = table.concat({out, i, " -> ", v.id, "\n"})
		end
		log:print(out)
	end,

	insert = function(key)
		local ag = key%k
		--lookup closest node in contacts at the relevant affinity group
		local closest = KELIPS_LOOKUP.findClosest(key, ag)
		--send insert request to the closest node
		if closest then
			local ok, res = rpc.acall(closest.peer, {'KELIPS_LOOKUP.insertKey', key})
			if ok then
				local homenode = res[1]
				if homenode then
					log:print("Key "..key.."(".. key%k..") inserted at node "..homenode.id.."("..homenode.id%k..")")
				end
			end
		end
	end,
	
	findClosest = function(key, ag)
		local closest = {}
		if TMAN.contacts[ag] then
			local ranked = KELIPS_LOOKUP.rank_clockwise(key, TMAN.contacts[ag])
			return ranked[1]
		end
	end,

	insertKey = function(key)	
		--log:print("Received key for insertion "..key)
		if TMAN.filetuples[key] == nil then
		--randomly pick a node from the affinity group
			local homenode = TMAN.aff_group[math.random(1,#TMAN.aff_group)]		
			--insert homenode and key in filetuples
			--log:print("Inserting key "..key.. "->" ..homenode.id)
			TMAN.filetuples[key] = homenode
			return homenode
		end
	end,

	activeThread = function()
		local partner = TMAN.aff_group[math.random(1,#TMAN.aff_group)]
		local buffer = KELIPS_LOOKUP.createMessage()
		local try = 0
		local ok, res = rpc.acall(partner.peer, {'KELIPS_LOOKUP.passiveThread', buffer})
		while not ok do
			try = try + 1
			if try <= 3 then
				log:print("Filetuples active thread: no response from:"..partner.id.. ": "..tostring(res).." => try again")
				events.sleep(math.random(try * 3, try * 6))
				ok, res = rpc.acall(partner.peer, {'KELIPS_LOOKUP.passiveThread', buffer})
			else
				log:print("KELIPS_LOOKUP active thread: no response from:"..partner.id..": "..tostring(res).."  => end")
			end
		end
		if ok then
			local received = res[1]
			KELIPS_LOOKUP.updateFileTuples(received)	
			--KELIPS_LOOKUP.display_ft()
		end
	end,

	passiveThread = function(received)
		local buffer = KELIPS_LOOKUP.createMessage()
		KELIPS_LOOKUP.updateFileTuples(received)
		return buffer
	end,

	createMessage = function()
	--log:print("Creating a message ...")
		--pick a random tuple from filetuples
		local keys = misc.table_keyset(TMAN.filetuples)
		local k = misc.random_pick(keys)
		--log:print("selected key: "..k)
		local hn = TMAN.filetuples[k]
		--log:print("selected hn: "..hn.id)
		buffer = {key = k, homenode = hn}
		return buffer
	end,

	updateFileTuples = function(received)
		local key = received.key
		local homenode = received.homenode
		if TMAN.filetuples[key] == nil then
			--log:print("Updating filetuples with the received: "..received.key.. "->".. homenode.id)
			TMAN.filetuples[key] = homenode
		end
	end,

	initFileTuples = function()
		local key = me.id
		TMAN.filetuples[key] = me
	end,

	rank_clockwise = function(key, set)
		local distances = {}
		local ranked = {}
		for i,v in ipairs(set) do
			local dist = v.id%n - key%n 
			local d = 0
			if dist >= 0 then d = dist
			else d = dist + n end
			distances[#distances+1] = {distance= d, node=v}
		end
		table.sort(distances, function(a,b) return a.distance < b.distance end)
		for i,v in ipairs(distances) do
			ranked[#ranked+1] = v.node
		end
		return ranked
	end,

	lookup = function(key)
		local ag = key%k
		local closest = KELIPS_LOOKUP.findClosest(key, ag)
		local try = 0
		local ok, res = rpc.acall(closest.peer, {'KELIPS_LOOKUP.findKey', key})
		if ok then
			local homenode = res[1]
			if homenode then log:print("Key "..key.."(".. key%k..") found at node "..homenode.id.."("..homenode.id%k..")")
			else log:print("Key "..key.."(".. key%k..") not found") end
		end
	end,

	findKey = function(key)
		return TMAN.filetuples[key]
	end,
	
	test_insert = function()
		local index = job.position%n + 1
		local a_peer = job.nodes[index]
		local key = compute_hash(table.concat({tostring(a_peer.ip),":",tostring(a_peer.port)}))+index
		log:print("Starting key insertion: "..key)
		KELIPS_LOOKUP.insert(key)
	end,
	
	test_lookup = function()
		local index = job.position%n + 1
		local a_peer = job.nodes[index]
		local key = compute_hash(table.concat({tostring(a_peer.ip),":",tostring(a_peer.port)}))+index
		log:print("Starting key lookup: "..key)
		KELIPS_LOOKUP.lookup(key)
	end

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
	tman_thread = events.periodic(GOSSIP_TIME, TMAN.activeThread)
	
	events.sleep(100)
	
	KELIPS_LOOKUP.initFileTuples()
	KELIPS_LOOKUP.test_insert()
	events.sleep(20)
	kelips_thread = events.periodic(GOSSIP_TIME, KELIPS_LOOKUP.activeThread)
	events.sleep(120)
	KELIPS_LOOKUP.test_lookup()
	events.kill({tman_thread, kelips_thread})
			
end  

events.thread(main)  
events.loop()
