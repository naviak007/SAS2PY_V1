document.getElementById("sendBtn")
.addEventListener("click", sendChat)

async function sendChat(){

const input = document.getElementById("chatInput")

const message = input.value.trim()

if(!message) return

input.value = ""

await sendToIgnis(message)

}