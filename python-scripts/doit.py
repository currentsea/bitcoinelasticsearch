itemList = []
with open("list.txt", "r") as theList: 
	for item in theList: 
		itemList.append(item.strip())
print itemList
	