import nodeMailer from 'nodemailer';
import { google } from 'googleapis';
import dotenv from 'dotenv';
dotenv.config();

const clientId = process.env.CLIENT_ID;
const clientSecret = process.env.CLIENT_SECRET;
const refreshToken = process.env.REFRESH_TOKEN;
const mail = process.env.MAIL;

const OAuth2 = google.auth.OAuth2;
const client = new OAuth2(clientId, clientSecret);
client.setCredentials({ refresh_token: refreshToken });

export default function sendMail(recipient, subject, message) {
  const accessToken = client.getAccessToken();

  let transport = nodeMailer.createTransport({
    service: 'gmail',
    auth: {
      type: 'OAUTH2',
      user: mail,
      refreshToken,
      accessToken,
      clientId,
      clientSecret
    }
  });

  const mail_format = {
    from: mail,
    to: recipient,
    subject,
    html: message
  };

  transport.sendMail(mail_format, (err, result) => {
    if (err) throw new Error(err);
  });
}
