import { useState, useCallback, useId } from 'react';

export interface RevealButtonProps {
  /** Label shown on the button when content is hidden */
  label?: string;
  /** Label shown on the button when content is revealed */
  hideLabel?: string;
  /** Content to show/hide */
  children: React.ReactNode;
  /** Optional callback fired when content is revealed */
  onReveal?: () => void;
  /** Additional CSS classes for the container */
  className?: string;
}

/**
 * RevealButton - A reusable reveal toggle component.
 * Shows a button that, when clicked, reveals hidden content with a slide-down/fade-in animation.
 * Requirement 3.3, 14.4: Animated reveal with slide-down/fade-in (200–400ms).
 */
export default function RevealButton({
  label = 'Show Answer',
  hideLabel = 'Hide Answer',
  children,
  onReveal,
  className = '',
}: RevealButtonProps) {
  const [isRevealed, setIsRevealed] = useState(false);
  const contentId = useId();

  const handleToggle = useCallback(() => {
    if (!isRevealed && onReveal) {
      onReveal();
    }
    setIsRevealed((prev) => !prev);
  }, [isRevealed, onReveal]);

  return (
    <div className={className}>
      <button
        type="button"
        onClick={handleToggle}
        aria-expanded={isRevealed}
        aria-controls={contentId}
        className="inline-flex items-center gap-[8px] px-[16px] py-[8px] rounded-md
          bg-primary text-white font-medium text-body-sm
          hover:bg-primary-dark focus:outline-none focus:ring-2 focus:ring-primary focus:ring-offset-2
          interactive min-h-touch min-w-touch cursor-pointer
          transition-colors duration-200"
      >
        <svg
          className={`w-4 h-4 transition-transform duration-200 ${isRevealed ? 'rotate-180' : ''}`}
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
          strokeWidth={2}
          aria-hidden="true"
        >
          <path strokeLinecap="round" strokeLinejoin="round" d="M19 9l-7 7-7-7" />
        </svg>
        {isRevealed ? hideLabel : label}
      </button>

      <div
        id={contentId}
        role="region"
        aria-hidden={!isRevealed}
        className={`mt-[12px] ${isRevealed ? 'reveal-slide-down' : 'hidden'}`}
      >
        {children}
      </div>
    </div>
  );
}
