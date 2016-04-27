import logging
import random

logger = logging.getLogger(__name__)


class Messenger(object):
    def __init__(self, slack_clients):
        self.clients = slack_clients

    def send_message(self, channel_id, msg):
        # in the case of Group and Private channels, RTM channel payload is a complex dictionary
        if isinstance(channel_id, dict):
            channel_id = channel_id['id']
        logger.debug('Sending msg: {} to channel: {}'.format(msg, channel_id))
        channel = self.clients.rtm.server.channels.find(channel_id)
        channel.send_message("{}".format(msg.encode('ascii', 'ignore')))

    def write_help_message(self, channel_id):
        bot_uid = self.clients.bot_user_id()
        txt = '{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}'.format(
            "I'm a bot that demonstrates various conversational dialogs in the medical domain:",
            "*Acronym Definitions*",
            "> _*User:*_ What does ESV stand for?",
            "> _*Bot:*_ `esv` stands for: _end-systolic volume_",
            ">    * _Bot knows the following acronyms: (EDV, ESV, EF, SV, Mass)_",
            "*Cardiac Anatomical Normal Values for a particular Patient Demographic*",
            "> _*User:*_ What is the normal LV ESV for a 36 year old female?",
            "> _*Bot:*_ The normal absolute esv value is 40 (ml) +/- 12 or within the range from 16 to 64",
            "> _*User:*_ What is the normal right ventricle ejection fraction for a 22 year old male?",
            "> _*Bot:*_ The normal absolute ef value is 61 (%) +/- 6 or within the range from 49 to 73",
            "*Billing Code for a Procedure*",
            "> _*User:*_ How can I find the CPT billing code to the MRI procedure we just preformed?",
            "> _*Bot:*_ I can help with that.  Could you let me know what anatomic part it involved?",
            "> _*User:*_ heart",
            "> _*Bot:*_ Was contrast agent used?",
            "> _*User:*_ no",
            "> _*Bot:*_ Was the patient scanned using stress imaging?",
            "> _*User:*_ yes",
            "> _*Bot:*_ The CPT code you are interested in is:\n>> *75559* - _Cardiac magnetic resonance imaging for morphology and function without contrast material; with stress imaging_"
        )
        self.send_message(channel_id, txt)

    def write_greeting(self, channel_id, user_id):
        greetings = ['Hi', 'Hello', 'Nice to meet you', 'Howdy', 'Salutations']
        txt = '{}, <@{}>!'.format(random.choice(greetings), user_id)
        self.send_message(channel_id, txt)

    def write_prompt(self, channel_id):
        bot_uid = self.clients.bot_user_id()
        txt = "I'm sorry, I didn't quite understand... Can I help you? (e.g. `<@" + bot_uid + "> help`)"
        self.send_message(channel_id, txt)

    def write_joke(self, channel_id):
        question = "Why did the python cross the road?"
        self.send_message(channel_id, question)
        self.clients.send_user_typing_pause(channel_id)
        answer = "To eat the chicken on the other side! :laughing:"
        self.send_message(channel_id, answer)


    def write_error(self, channel_id, err_msg):
        txt = ":face_with_head_bandage: my maker didn't handle this error very well:\n>```{}```".format(err_msg)
        self.send_message(channel_id, txt)

    def demo_attachment(self, channel_id):
        txt = "Beep Beep Boop is a ridiculously simple hosting platform for your Slackbots."
        attachment = {
            "pretext": "We bring bots to life. :sunglasses: :thumbsup:",
            "title": "Host, deploy and share your bot in seconds.",
            "title_link": "https://beepboophq.com/",
            "text": txt,
            "fallback": txt,
            "image_url": "https://storage.googleapis.com/beepboophq/_assets/bot-1.22f6fb.png",
            "color": "#7CD197",
        }
        self.clients.web.chat.post_message(channel_id, txt, attachments=[attachment], as_user='true')
