import { useState, useCallback, useRef } from 'react';
import RevealButton from './RevealButton';
import { progressService } from '../../services/progressService';

export interface CodeBlockProps {
  code: string;
  language: string;
  showLineNumbers: boolean;
  maxHeight?: number;
}

export interface ScenarioQuestionProps {
  id: string;
  scenario: string;
  interviewerTesting: string;
  detailedAnswer: string;
  codeSnippets: CodeBlockProps[];
  diagrams: string[];
  followUpQuestions: { question: string; answer: string }[];
  difficultyLevel: 'junior' | 'mid-level' | 'senior';
}

/**
 * Maps difficulty level to badge styling.
 */
function getDifficultyBadge(level: ScenarioQuestionProps['difficultyLevel']) {
  const styles: Record<typeof level, { bg: string; text: string; label: string }> = {
    junior: {
      bg: 'bg-success-light',
      text: 'text-success-dark',
      label: 'Junior',
    },
    'mid-level': {
      bg: 'bg-warning-light',
      text: 'text-warning-dark',
      label: 'Mid-Level',
    },
    senior: {
      bg: 'bg-danger-light',
      text: 'text-danger-dark',
      label: 'Senior',
    },
  };
  return styles[level];
}

/**
 * ScenarioCard - Displays a scenario-based interview question with reveal functionality.
 *
 * Initial state: Shows scenario description + interviewer-testing text only.
 * Hidden behind reveal: detailed answer, code snippets, diagrams.
 * On reveal of answer: calls ProgressService.markAttempted().
 * Follow-up questions have independent reveal toggles.
 *
 * Requirements: 3.2, 3.3, 3.5, 6.2, 14.4
 */
export default function ScenarioCard({
  id,
  scenario,
  interviewerTesting,
  detailedAnswer,
  codeSnippets,
  diagrams,
  followUpQuestions,
  difficultyLevel,
}: ScenarioQuestionProps) {
  const [answerRevealed, setAnswerRevealed] = useState(false);
  const hasMarkedAttempted = useRef(false);

  const badge = getDifficultyBadge(difficultyLevel);

  const handleAnswerReveal = useCallback(() => {
    if (!hasMarkedAttempted.current) {
      progressService.markAttempted(id);
      hasMarkedAttempted.current = true;
    }
    setAnswerRevealed(true);
  }, [id]);

  return (
    <article
      className="rounded-lg shadow-card bg-surface p-[24px] border border-surface-secondary
        hover-lift"
      aria-label={`Scenario question: ${scenario.slice(0, 60)}`}
    >
      {/* Header with difficulty badge */}
      <div className="flex items-start justify-between gap-[16px] mb-[16px]">
        <h3 className="text-heading-4 text-content flex-1">Scenario Question</h3>
        <span
          className={`inline-flex items-center px-[12px] py-[4px] rounded-full text-caption font-medium ${badge.bg} ${badge.text}`}
        >
          {badge.label}
        </span>
      </div>

      {/* Scenario description - always visible */}
      <div className="mb-[16px]">
        <h4 className="text-subheading text-content mb-[8px] font-medium">Scenario</h4>
        <p className="text-body text-content-secondary leading-relaxed">{scenario}</p>
      </div>

      {/* Interviewer testing - always visible */}
      <div className="mb-[16px]">
        <h4 className="text-subheading text-content mb-[8px] font-medium">
          What the Interviewer is Testing
        </h4>
        <p className="text-body text-content-secondary leading-relaxed">{interviewerTesting}</p>
      </div>

      {/* Reveal button for answer, code snippets, and diagrams */}
      <RevealButton
        label="Reveal Answer"
        hideLabel="Hide Answer"
        onReveal={handleAnswerReveal}
        className="mb-[16px]"
      >
        {/* Detailed answer */}
        <div className="mb-[16px]">
          <h4 className="text-subheading text-content mb-[8px] font-medium">Detailed Answer</h4>
          <div className="text-body text-content-secondary leading-relaxed whitespace-pre-wrap">
            {detailedAnswer}
          </div>
        </div>

        {/* Code snippets */}
        {codeSnippets.length > 0 && (
          <div className="mb-[16px]">
            <h4 className="text-subheading text-content mb-[8px] font-medium">Code Examples</h4>
            <div className="space-y-[12px]">
              {codeSnippets.map((snippet, index) => (
                <div
                  key={index}
                  className="rounded-md bg-surface-tertiary border border-surface-secondary overflow-hidden"
                >
                  {/* Language label */}
                  <div className="flex items-center justify-between px-[12px] py-[4px] bg-surface-secondary border-b border-surface-secondary">
                    <span className="text-caption font-medium text-content-secondary uppercase">
                      {snippet.language || 'text'}
                    </span>
                  </div>
                  {/* Code content */}
                  <pre
                    className={`p-[12px] overflow-x-auto text-body-sm font-mono ${
                      snippet.maxHeight ? `max-h-[${snippet.maxHeight}px] overflow-y-auto` : ''
                    }`}
                    style={
                      snippet.code.split('\n').length > 30
                        ? { maxHeight: snippet.maxHeight || 500, overflowY: 'auto' }
                        : undefined
                    }
                  >
                    <code className={`language-${snippet.language || 'text'}`}>
                      {snippet.code}
                    </code>
                  </pre>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Diagrams */}
        {diagrams.length > 0 && (
          <div className="mb-[16px]">
            <h4 className="text-subheading text-content mb-[8px] font-medium">Diagrams</h4>
            <div className="space-y-[12px]">
              {diagrams.map((diagram, index) => (
                <div
                  key={index}
                  className="rounded-md bg-surface-tertiary border border-surface-secondary p-[12px]"
                >
                  <pre className="text-body-sm font-mono whitespace-pre-wrap overflow-x-auto">
                    {diagram}
                  </pre>
                </div>
              ))}
            </div>
          </div>
        )}
      </RevealButton>

      {/* Follow-up probe questions - independent reveals */}
      {followUpQuestions.length > 0 && (
        <div className="mt-[16px] pt-[16px] border-t border-surface-secondary">
          <h4 className="text-subheading text-content mb-[12px] font-medium">
            Follow-Up Probe Questions
          </h4>
          <div className="space-y-[12px]">
            {followUpQuestions.map((fq, index) => (
              <div
                key={index}
                className="rounded-md bg-surface-secondary p-[16px]"
              >
                <p className="text-body text-content font-medium mb-[8px]">
                  {index + 1}. {fq.question}
                </p>
                <RevealButton
                  label="Show Answer"
                  hideLabel="Hide Answer"
                  className="ml-[4px]"
                >
                  <p className="text-body text-content-secondary leading-relaxed">
                    {fq.answer}
                  </p>
                </RevealButton>
              </div>
            ))}
          </div>
        </div>
      )}
    </article>
  );
}
