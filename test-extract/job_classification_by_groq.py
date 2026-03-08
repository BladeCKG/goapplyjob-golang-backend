import argparse
import asyncio
from pathlib import Path
import sys

from dotenv import load_dotenv
from sqlalchemy import select

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.db.models import ParsedJob, RawUsJob
from app.db.session import SessionLocal
from app.workers.process_parsed_jobs import _normalize_tech_stack
from groq import Groq


async def run() -> None:

  client = Groq()
  completion = client.chat.completions.create(
      model="llama-3.1-8b-instant",
      messages=[
        {
          "role": "user",
          "content": "You are a job title classification system.\n\nYour task is to assign the MOST suitable job category for a given job title.\n\nInstructions:\n- You MUST select exactly ONE category from the list below.\n- Do NOT create new categories.\n- Choose the closest functional match based on role, domain, and seniority.\n- If multiple categories are similar, prefer the more specific one (e.g., \"Backend Engineer\" over \"Software Engineer\").\n- If no strong match exists, choose \"Blank\".\n- Return ONLY valid JSON.\n\nJob title: \"Product Implementation Engineer\"\n\nCategories:\nLearning and Development, Consultant, Actuary, Account Manager, Project Manager, Vice President, Data Engineer, Product Operations, Analyst, Customer Support, Procurement, Recruitment, Platform Engineer, Network Engineer, Full-stack Engineer, Manager, Auditor, Designer, Financial Planning and Analysis, Account Executive, Business Development Rep, Supply Chain, Insurance, Underwriter, Client Services Representative, Data Analyst, Director, People Operations, Medical Billing and Coding, IT Support, Business Analyst, Business Intelligence Analyst, Product Manager, Therapist, Payroll, Salesforce Administrator, Business Operations, Security Analyst, QA Engineer, Marketing, Customer Success Manager, Operations, Administration, Brand Designer, Research Analyst, Translator, Outside Sales, Tax, Compliance, Architect, Attorney, Collections, Support Engineer, DevOps Engineer, Graphics Designer, Security Engineer, Sales, Machine Learning Engineer, System Administrator, Claims Specialist, Controller, Clinical Research, Ecommerce, Influencer Marketing, Solutions Engineer, Data Scientist, Inside Sales, Field Engineer, Producer, Infrastructure Engineer, General Counsel, Paralegal, Product Designer, Software Engineer, Engineer, Counselor, Technical Product Manager, Accounts Receivable, Revenue Operations, Artificial Intelligence, Human Resources, Backend Engineer, Technical Recruiter, Onboarding Specialist, Mechanical Engineer, Program Manager, Performance Marketing, Accountant, Pricing Analyst, SEO Marketing, Chief Technology Officer, Systems Engineer, Growth Marketing, Engineering Manager, ServiceNow, Content Writer, Affiliate Manager, Clinical Operations, Application Engineer, Art Director, Network Operations, Product Specialist, Cloud Engineer, Research Scientist, Communications, Chief Marketing Officer, 3D Artist, Events, Copywriter, Business Intelligence Developer, Executive Assistant, Implementation Specialist, SDET, Frontend Engineer, Digital Marketing, Medical Director, Database Administrator, Civil Engineer, Analytics Engineer, Scrum Master, Call Center Representative, AI Engineer, Sales Engineer, Technical Program Manager, Sales Operations Manager, Marketing Operations, Social Media Manager, Creative Strategist, SAP, Conversion Rate Optimizer, Community Manager, Sales Development Rep, Content Creator, Email Marketing Manager, Content Marketing Manager, Accounting Manager, Android Engineer, Risk, Security Operations, Product Marketing, Bilingual, Blockchain Engineer, Strategy, Pre-sales Engineer, Administrative Assistant, Technical Account Manager, Customer Advocate, Threat Intelligence Specialist, Journalist, Brand Manager, Data Entry, Accounts Payable, Product Analyst, Capture Manager, Video Editor, User Researcher, Chief of Staff, Medical writer, Proposal Manager, QA Automation Engineer, Notary, Technical Customer Success, Technical Writer, Financial Crime, Technical Project Manager, Client Partner, Marketing Analyst, Crypto, Medical Reviewer, Brand Ambassador, Electrical Engineer, Hardware Engineer, Lead Generation, NLP Engineer, Developer Relations, Robotics, Bookkeeper, Web Designer, Salesforce Developer, Game Engineer, iOS Engineer, Public Relations, Salesforce Analyst, Billing Specialist, Chief Operating Officer, Incident Response Analyst, Smart Contract Engineer, AI Research Scientist, Content Manager, Legal Assistant, Research Engineer, LLM Engineer, Customer Retention Specialist, Computer Vision Engineer, Production Engineer, Salesforce Consultant, Product Adoption Specialist, Appointment Setter, DeFi, 2D Artist, Web3, Blank\n\nOutput format:\n{\n  \"category\": \"<exact category from list>\",\n  \"confidence\": 0.0-1.0\n}"
        },
      ],
      temperature=1,
      max_completion_tokens=1024,
      top_p=1,
      stream=True,
      stop=None
  )

  for chunk in completion:
      print(chunk.choices[0].delta.content or "", end="")


def main() -> None:

    load_dotenv(PROJECT_ROOT / ".env")

    asyncio.run(run())


if __name__ == "__main__":
    main()

